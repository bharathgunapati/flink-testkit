package io.flinktestkit.example;

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
 * End-to-end demonstration of flink-testkit: submits the real
 * {@link UppercaseJob} against a containerized Kafka broker, produces
 * input via a typed {@link TopicHandle}, and asserts on the job's output
 * with no {@code Thread.sleep}, no raw byte[] handling, and no manual
 * topic/consumer-group bookkeeping.
 *
 * <p>Topics are shared across {@code @Test} methods in this class (per-class
 * lifecycle), so assertions wait for specific order IDs rather than assuming
 * an empty topic.
 */
@ExtendWith(FlinkTestExtension.class)
class UppercaseJobTest {

    @KafkaTopic(valueType = OrderEvent.class)
    static TopicHandle<OrderEvent> orders;

    @KafkaTopic(valueType = EnrichedOrder.class)
    static TopicHandle<EnrichedOrder> enrichedOrders;

    @Test
    void uppercasesCustomerName() throws Exception {
        JobClient jobClient = UppercaseJob.runAsync(
            orders.bootstrapServers(), orders.topicName(), enrichedOrders.topicName());

        try {
            orders.produce(new OrderEvent("order-1", "alice", 42.50));

            List<EnrichedOrder> results = enrichedOrders.awaitRecords(
                records -> records.stream().anyMatch(r -> "order-1".equals(r.orderId())),
                Duration.ofSeconds(30));

            EnrichedOrder result = results.stream()
                .filter(r -> "order-1".equals(r.orderId()))
                .findFirst()
                .orElseThrow();
            assertThat(result.customerName()).isEqualTo("ALICE");
            assertThat(result.amount()).isEqualTo(42.50);
        } finally {
            jobClient.cancel();
        }
    }

    @Test
    void handlesMultipleOrdersInOneBatch() throws Exception {
        JobClient jobClient = UppercaseJob.runAsync(
            orders.bootstrapServers(), orders.topicName(), enrichedOrders.topicName());

        try {
            orders.produce(
                new OrderEvent("order-2", "bob", 10.00),
                new OrderEvent("order-3", "carol", 99.99));

            List<EnrichedOrder> results = enrichedOrders.awaitRecords(
                records -> records.stream().anyMatch(r -> "order-2".equals(r.orderId()))
                    && records.stream().anyMatch(r -> "order-3".equals(r.orderId())),
                Duration.ofSeconds(30));

            assertThat(results)
                .filteredOn(r -> r.orderId().equals("order-2") || r.orderId().equals("order-3"))
                .extracting(EnrichedOrder::customerName)
                .containsExactlyInAnyOrder("BOB", "CAROL");
        } finally {
            jobClient.cancel();
        }
    }
}
