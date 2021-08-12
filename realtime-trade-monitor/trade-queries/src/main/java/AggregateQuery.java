import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.kinesis.KinesisSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;

public class AggregateQuery {

    public static final String STREAM = "trades";

    public static void aggregateQuery(HazelcastInstance hzInstance, String servers) {
        try {
            JobConfig query1config = new JobConfig()
                    .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                    .setName("AggregateQuery")
                    .addClass(TradeJsonDeserializer.class)
                    .addClass(Trade.class)
                    .addClass(AggregateQuery.class);

            JetService jetService = hzInstance.getJet();
            jetService.newJobIfAbsent(createPipeline(servers), query1config);

        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static Pipeline createPipeline(String servers) {
        Pipeline p = Pipeline.create();

        StreamSource<Entry<String, byte[]>> kinesisSource = KinesisSources.kinesis(STREAM)
                .withRegion(System.getenv("AWS_REGION"))
                .withCredentials(System.getenv("AWS_ACCESS_KEY"), System.getenv("AWS_SECRET_KEY"))
                .withInitialShardIteratorRule(".*", "LATEST", null)
                .withEndpoint("https://kinesis.us-west-2.amazonaws.com")
                .build();

        p.readFrom(kinesisSource)
                .withoutTimestamps()
                .setLocalParallelism(2)
                .map(record -> JsonUtil.beanFrom(new String(record.getValue()), Trade.class))
                .groupingKey(Trade::getSymbol)
                .rollingAggregate(allOf(
                        counting(),
                        summingLong(trade -> (long) trade.getPrice() * trade.getQuantity()),
                        latestValue(Trade::getPrice)
                ))
                .setName("aggregate by symbol")
                .writeTo(Sinks.map("query1_Results"));
        return p;
    }

    private static <T, R> AggregateOperation1<T, ?, R> latestValue(FunctionEx<T, R> toValueFn) {
        return AggregateOperation.withCreate((SupplierEx<MutableReference<R>>) MutableReference::new)
                .<T>andAccumulate((ref, t) -> ref.set(toValueFn.apply(t)))
                .andExportFinish(MutableReference::get);
    }
}
