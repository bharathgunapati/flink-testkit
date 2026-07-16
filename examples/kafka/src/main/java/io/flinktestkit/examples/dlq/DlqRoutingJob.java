package io.flinktestkit.examples.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.flinktestkit.examples.orderenrichment.EnrichedOrder;
import io.flinktestkit.examples.orderenrichment.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Kafka job that routes unparseable payloads to a DLQ topic.
 * Consumer-style example under {@code examples/kafka}.
 */
public final class DlqRoutingJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq", Types.STRING);

    private DlqRoutingJob() {
    }

    public static JobClient runAsync(
            String bootstrapServers,
            String inputTopic,
            String outputTopic,
            String dlqTopic) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("examples-dlq-routing-" + System.nanoTime())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSink<String> mainSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(dlqTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        DataStream<String> input = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "orders-source");

        SingleOutputStreamOperator<String> processed = input.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) {
                try {
                    OrderEvent order = MAPPER.readValue(value, OrderEvent.class);
                    EnrichedOrder enriched = new EnrichedOrder(
                        order.orderId(), order.customerName().toUpperCase(), order.amount());
                    out.collect(MAPPER.writeValueAsString(enriched));
                } catch (Exception e) {
                    ctx.output(DLQ_TAG, value);
                }
            }
        });

        processed.sinkTo(mainSink);
        processed.getSideOutput(DLQ_TAG).sinkTo(dlqSink);

        return env.executeAsync("examples-dlq-routing");
    }
}
