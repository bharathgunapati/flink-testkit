package io.flinktestkit.example;

import io.flinktestkit.example.avro.EnrichedPayment;
import io.flinktestkit.example.avro.PaymentEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Avro + Schema Registry example: uppercases {@link PaymentEvent#getMerchant()}
 * and writes {@link EnrichedPayment}. Used only as fixture code for
 * {@link AvroPaymentJobTest}.
 */
public final class AvroPaymentJob {

    private AvroPaymentJob() {
    }

    public static JobClient runAsync(
            String bootstrapServers,
            String schemaRegistryUrl,
            String inputTopic,
            String outputTopic) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<PaymentEvent> source = KafkaSource.<PaymentEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("avro-payment-job-" + System.nanoTime())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                    PaymentEvent.class, schemaRegistryUrl))
            .build();

        KafkaSink<EnrichedPayment> sink = KafkaSink.<EnrichedPayment>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(
                    ConfluentRegistryAvroSerializationSchema.forSpecific(
                        EnrichedPayment.class, outputTopic, schemaRegistryUrl))
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "payments")
            .map(AvroPaymentJob::enrich)
            .sinkTo(sink);

        return env.executeAsync("avro-payment-job");
    }

    private static EnrichedPayment enrich(PaymentEvent payment) {
        return EnrichedPayment.newBuilder()
            .setPaymentId(payment.getPaymentId())
            .setMerchant(payment.getMerchant().toUpperCase())
            .setAmount(payment.getAmount())
            .build();
    }
}
