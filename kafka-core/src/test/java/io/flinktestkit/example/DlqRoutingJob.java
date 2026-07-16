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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Reads JSON strings from Kafka, parses {@link OrderEvent}, uppercases the
 * customer name into {@link EnrichedOrder}, and routes unparseable payloads
 * to a DLQ side output. Fixture for {@link DlqRoutingJobTest}.
 */
public final class DlqRoutingJob {

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
            .setGroupId("dlq-routing-job-" + System.nanoTime())
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
            source, WatermarkStrategy.noWatermarks(), "orders-with-poison");

        SingleOutputStreamOperator<String> processed = input.process(new ParseOrDlq());

        processed.sinkTo(mainSink);
        processed.getSideOutput(DLQ_TAG).sinkTo(dlqSink);

        return env.executeAsync("dlq-routing-job");
    }

    private static final class ParseOrDlq extends ProcessFunction<String, String> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public void processElement(String json, Context ctx, Collector<String> out) {
            try {
                OrderEvent order = MAPPER.readValue(json, OrderEvent.class);
                EnrichedOrder enriched = new EnrichedOrder(
                    order.orderId(), order.customerName().toUpperCase(), order.amount());
                out.collect(MAPPER.writeValueAsString(enriched));
            } catch (Exception e) {
                ctx.output(DLQ_TAG, json);
            }
        }
    }
}
