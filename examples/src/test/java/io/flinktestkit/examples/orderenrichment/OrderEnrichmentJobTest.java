package io.flinktestkit.examples.orderenrichment;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer-style MiniCluster test: depends on {@code flink-testkit-kafka-core}
 * with {@code test} scope, wires the real {@link OrderEnrichmentJob} to
 * Testcontainers Kafka via {@link TopicHandle}.
 */
@ExtendWith(FlinkTestExtension.class)
class OrderEnrichmentJobTest {

    @KafkaTopic(valueType = OrderEvent.class)
    static TopicHandle<OrderEvent> orders;

    @KafkaTopic(valueType = EnrichedOrder.class)
    static TopicHandle<EnrichedOrder> enrichedOrders;

    @Test
    void uppercasesCustomerNameOnMiniCluster() throws Exception {
        JobClient jobClient = OrderEnrichmentJob.runAsync(
            orders.bootstrapServers(),
            orders.topicName(),
            enrichedOrders.topicName());

        try {
            orders.produce(new OrderEvent("order-1", "alice", 42.50));

            List<EnrichedOrder> results = enrichedOrders.awaitRecords(
                records -> records.stream().anyMatch(r -> "order-1".equals(r.orderId())),
                Duration.ofSeconds(30));

            assertThat(results)
                .filteredOn(r -> "order-1".equals(r.orderId()))
                .extracting(EnrichedOrder::customerName)
                .containsExactly("ALICE");
        } finally {
            try {
                jobClient.cancel().get();
            } catch (Exception ignored) {
                // Job may already have finished for a bounded source.
            }
        }
    }
}
