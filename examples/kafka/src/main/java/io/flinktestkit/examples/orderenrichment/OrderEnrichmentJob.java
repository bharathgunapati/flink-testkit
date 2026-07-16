package io.flinktestkit.examples.orderenrichment;

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
 * Sample Flink job shaped like application code in a real project:
 * Kafka source → transform → Kafka sink, submitted on the MiniCluster via
 * {@link StreamExecutionEnvironment#executeAsync(String)}.
 *
 * <p>Not part of the published flink-testkit library — lives under
 * {@code examples/} to show consumer usage.
 */
public final class OrderEnrichmentJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OrderEnrichmentJob() {
    }

    public static JobClient runAsync(String bootstrapServers, String inputTopic, String outputTopic)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("order-enrichment-job")
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

        input.map(OrderEnrichmentJob::enrich).sinkTo(sink);

        return env.executeAsync("order-enrichment-job");
    }

    private static String enrich(String json) throws Exception {
        OrderEvent order = MAPPER.readValue(json, OrderEvent.class);
        EnrichedOrder enriched = new EnrichedOrder(
            order.orderId(), order.customerName().toUpperCase(), order.amount());
        return MAPPER.writeValueAsString(enriched);
    }
}
