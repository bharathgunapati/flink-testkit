package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.Format;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import io.flinktestkit.example.avro.EnrichedPayment;
import io.flinktestkit.example.avro.PaymentEvent;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3: end-to-end Avro + Schema Registry against a real Flink job.
 * Declaring {@code format = Format.AVRO} is enough to start Schema Registry
 * and wire Confluent serdes on the {@link TopicHandle}.
 */
@ExtendWith(FlinkTestExtension.class)
class AvroPaymentJobTest {

    @KafkaTopic(valueType = PaymentEvent.class, format = Format.AVRO)
    static TopicHandle<PaymentEvent> payments;

    @KafkaTopic(valueType = EnrichedPayment.class, format = Format.AVRO)
    static TopicHandle<EnrichedPayment> enriched;

    @Test
    void uppercasesMerchantViaAvroAndSchemaRegistry() throws Exception {
        JobClient job = AvroPaymentJob.runAsync(
            payments.bootstrapServers(),
            payments.schemaRegistryUrl(),
            payments.topicName(),
            enriched.topicName());

        try {
            payments.produce(PaymentEvent.newBuilder()
                .setPaymentId("pay-1")
                .setMerchant("acme")
                .setAmount(19.99)
                .build());

            List<EnrichedPayment> results = enriched.awaitRecords(
                records -> records.stream().anyMatch(r -> "pay-1".equals(r.getPaymentId())),
                Duration.ofSeconds(60));

            EnrichedPayment result = results.stream()
                .filter(r -> "pay-1".equals(r.getPaymentId()))
                .findFirst()
                .orElseThrow();
            assertThat(result.getMerchant()).isEqualTo("ACME");
            assertThat(result.getAmount()).isEqualTo(19.99);
            assertThat(payments.schemaRegistryUrl()).startsWith("http://");
            assertThat(payments.schemaRegistryUrl()).isEqualTo(enriched.schemaRegistryUrl());
        } finally {
            job.cancel();
        }
    }
}
