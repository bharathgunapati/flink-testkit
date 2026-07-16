package io.flinktestkit.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Forwards {@link OrderEvent} JSON only when the Kafka record carries
 * {@code route=priority}. Demonstrates header-aware produce/assert flows
 * against a real Flink job.
 */
public final class PriorityHeaderJob {

    private PriorityHeaderJob() {
    }

    public static JobClient runAsync(String bootstrapServers, String inputTopic, String outputTopic)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("priority-header-job-" + System.nanoTime())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new PriorityOnlyDeserializer())
            .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "priority-orders")
            .sinkTo(sink);

        return env.executeAsync("priority-header-job");
    }

    private static final class PriorityOnlyDeserializer
            implements KafkaRecordDeserializationSchema<String> {

        private final DeserializationSchema<String> valueSchema = new SimpleStringSchema();

        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            valueSchema.open(context);
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out)
                throws IOException {
            var header = record.headers().lastHeader("route");
            if (header == null || header.value() == null) {
                return;
            }
            if (!"priority".equals(new String(header.value(), StandardCharsets.UTF_8))) {
                return;
            }
            if (record.value() != null) {
                out.collect(valueSchema.deserialize(record.value()));
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
