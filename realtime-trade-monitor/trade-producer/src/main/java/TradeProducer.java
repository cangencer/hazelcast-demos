import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class TradeProducer {

    private static final int MAX_BATCH_SIZE = 16 * 1024;
    private static final int QUANTITY = 10_000;
    private static final String STREAM = "trades";

    private final int rate;
    private final Map<String, Integer> symbolToPrice;
    private final List<String> symbols;
    private final AmazonKinesis kinesisClient;

    private long emitSchedule;

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 0) {
            System.out.println("TradeProducer <rate>");
            System.exit(1);
        }
        int rate = Integer.parseInt(args[0]);

        new TradeProducer(rate, loadSymbols()).run();
    }

    private TradeProducer(int rate, List<String> symbols) {
        this.rate = rate;
        this.symbols = symbols;
        this.symbolToPrice = symbols.stream().collect(Collectors.toMap(t -> t, t -> 2500));
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setRegion("us-west-2");
        this.kinesisClient = clientBuilder.build();

        this.emitSchedule = System.nanoTime();
    }

    private void run() throws InterruptedException {
        System.out.println("Producing " + rate + " trades per second");
        while (true) {
            long interval = TimeUnit.SECONDS.toNanos(1) / rate;
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            String seq = null;
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                if (System.nanoTime() < emitSchedule) {
                    break;
                }
                String symbol = symbols.get(rnd.nextInt(symbols.size()));
                int price = symbolToPrice.compute(symbol, (t, v) -> v + rnd.nextInt(-1, 2));
                String id = UUID.randomUUID().toString();
                String tradeLine = String.format("{" +
                                "\"id\": \"%s\"," +
                                "\"timestamp\": %d," +
                                "\"symbol\": \"%s\"," +
                                "\"price\": %d," +
                                "\"quantity\": %d" +
                                "}",
                        id,
                        System.currentTimeMillis(),
                        symbol,
                        price,
                        rnd.nextInt(10, QUANTITY)
                );
                PutRecordRequest putRequest  = new PutRecordRequest();
                putRequest.setStreamName(STREAM);
                putRequest.setPartitionKey(symbol);
                putRequest.setSequenceNumberForOrdering(seq);
                putRequest.setData(ByteBuffer.wrap(String.valueOf(tradeLine).getBytes()));
                PutRecordResult result = kinesisClient.putRecord(putRequest);
                seq = result.getSequenceNumber();
                System.out.println(result);
                emitSchedule += interval;
            }
            Thread.sleep(1);
        }
    }

    private static List<String> loadSymbols() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeProducer.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))
        ) {
            return reader.lines()
                         .skip(1)
                         .map(l -> l.split("\\|")[0])
                         .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
