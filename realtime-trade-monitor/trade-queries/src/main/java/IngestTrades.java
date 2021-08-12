import java.util.Map.Entry;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kinesis.KinesisSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;

import static com.hazelcast.jet.Util.entry;

public class IngestTrades {

    public static final String STREAM = "trades";

    public static void ingestTrades(HazelcastInstance hzInstance) {
        try {
            JobConfig ingestTradesConfig = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setName("ingestTrades")
                .addClass(IngestTrades.class);

            JetService jetService = hzInstance.getJet();
            jetService.newJobIfAbsent(createPipeline(), ingestTradesConfig);
        }
        finally {
            Hazelcast.shutdownAll();
        }
    }

    private static Pipeline createPipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<Entry<String, byte[]>> source = KinesisSources.kinesis(STREAM)
            .withRegion(System.getenv("AWS_REGION"))
            .withCredentials(System.getenv("AWS_ACCESS_KEY"), System.getenv("AWS_SECRET_KEY"))
            .withInitialShardIteratorRule(".*", "LATEST", null)
            .withEndpoint("https://kinesis.us-west-2.amazonaws.com")
            .build();
        p.readFrom(source)
            .withoutTimestamps()
            .setLocalParallelism(2)
            .map(record -> entry(record.getKey(), new HazelcastJsonValue(new String(record.getValue()))))
            .writeTo(Sinks.map("trades"));

        return p;
    }

}
