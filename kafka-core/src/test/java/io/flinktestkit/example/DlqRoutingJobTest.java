package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3.5: valid records flow to the output topic; poison payloads
 * produced via {@link TopicHandle#produceInvalid} land on the DLQ companion
 * topic declared with {@code @KafkaTopic(dlq = true)}.
 */
@ExtendWith(FlinkTestExtension.class)
class DlqRoutingJobTest {

    @KafkaTopic(valueType = OrderEvent.class, dlq = true)
    static TopicHandle<OrderEvent> orders;

    @KafkaTopic(valueType = EnrichedOrder.class)
    static TopicHandle<EnrichedOrder> enriched;

    @Test
    void routesPoisonMessagesToDlqAndKeepsValidOutput() throws Exception {
        JobClient job = DlqRoutingJob.runAsync(
            orders.bootstrapServers(),
            orders.topicName(),
            enriched.topicName(),
            orders.dlqTopicName());

        try {
            orders.produce(new OrderEvent("ok-1", "alice", 10.0));
            orders.produceInvalid("{this-is-not-json", "also-broken");

            List<EnrichedOrder> good = enriched.awaitRecords(
                records -> records.stream().anyMatch(r -> "ok-1".equals(r.orderId())),
                Duration.ofSeconds(45));

            assertThat(good)
                .filteredOn(r -> "ok-1".equals(r.orderId()))
                .first()
                .extracting(EnrichedOrder::customerName)
                .isEqualTo("ALICE");

            List<byte[]> poison = orders.awaitDlqRecords(2, Duration.ofSeconds(45));

            assertThat(poison)
                .extracting(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .contains("{this-is-not-json", "also-broken");
        } finally {
            job.cancel();
        }
    }
}
