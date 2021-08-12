import java.util.Objects;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.client.properties.ClientProperty.STATISTICS_ENABLED;

public class App {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Available commands:");
            System.out.println(" load-symbols");
            System.out.println(" ingest-trades <bootstrap servers>");
            System.out.println(" aggregate-query <bootstrap servers>");
            System.out.println(" benchmark-index");
            System.out.println(" benchmark-latency");
            System.exit(1);
        }

        String command = args[0];

        HazelcastInstance hzInstance = getHzInstance();
        try {
            if (command.equals("load-symbols")) {
                LoadSymbols.loadSymbols(hzInstance);
            } else if (command.equals("ingest-trades")) {
                IngestTrades.ingestTrades(hzInstance);
            } else if (command.equals("aggregate-query")) {
                AggregateQuery.aggregateQuery(hzInstance, args[1]);
            } else if (command.equals("benchmark-index")) {
                Benchmark.benchmark(hzInstance);
            } else if (command.equals("benchmark-latency")) {
                BenchmarkLatency.benchmark(hzInstance);
            }
        } finally {
            hzInstance.shutdown();
        }
    }

    private static HazelcastInstance getHzInstance() {
        ClientConfig config = new ClientConfig();
        config.setProperty(STATISTICS_ENABLED.getName(), "true");
        String discoveryToken = System.getenv("DISCOVERY_TOKEN");
        Objects.requireNonNull(discoveryToken, "DISCOVERY_TOKEN is not provided");
        String cloudUrl = System.getenv("CLOUD_URL");
        Objects.requireNonNull(cloudUrl, "CLOUD_URL is not provided");
        String clusterName = System.getenv("CLUSTER_NAME");
        Objects.requireNonNull(clusterName, "CLUSTER_NAME is not provided");
        config.setProperty(HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), discoveryToken);
        config.setProperty("hazelcast.client.cloud.url", cloudUrl);
        config.setClusterName(clusterName);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        System.out.println("Connection Successful!");
        return client;
    }

}
