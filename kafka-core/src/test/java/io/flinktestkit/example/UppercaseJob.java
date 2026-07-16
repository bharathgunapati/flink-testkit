package io.flinktestkit.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A deliberately trivial "real" Flink job, used only to demonstrate
 * flink-testkit against actual Flink code rather than a mock. It reads
 * {@link OrderEvent} JSON records from an input topic, uppercases the
 * customer name, and writes {@link EnrichedOrder} JSON records to an
 * output topic.
 *
 * <p>This class is not part of the flink-testkit library — it lives under
 * {@code src/test} purely as fixture code for {@link UppercaseJobTest}.
 */
public final class UppercaseJob {

    private UppercaseJob() {
    }

    /**
     * Submits the job and returns immediately with a {@link JobClient},
     * rather than blocking on {@code env.execute()} — this is a streaming
     * job with no natural end, so the caller (the test) is responsible for
     * cancelling it via the returned client once assertions are done.
     */
    public static JobClient runAsync(String bootstrapServers, String inputTopic, String outputTopic)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("uppercase-job")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        DataStream<String> input = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "orders-source");

        DataStream<String> output = input.map(UppercaseJob::enrich);

        output.sinkTo(sink);

        return env.executeAsync("uppercase-job");
    }

    private static String enrich(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        OrderEvent order = mapper.readValue(json, OrderEvent.class);
        EnrichedOrder enriched = new EnrichedOrder(
            order.orderId(), order.customerName().toUpperCase(), order.amount());
        return mapper.writeValueAsString(enriched);
    }
}
