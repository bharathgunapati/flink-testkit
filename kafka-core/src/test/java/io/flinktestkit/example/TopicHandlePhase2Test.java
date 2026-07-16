package io.flinktestkit.example;

import io.flinktestkit.ConsumedRecord;
import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 harness coverage without a Flink job: keys, headers, and
 * {@code awaitRecordsByKey} / {@code awaitConsumedRecords}.
 */
@ExtendWith(FlinkTestExtension.class)
class TopicHandlePhase2Test {

    @KafkaTopic(valueType = OrderEvent.class, partitions = 3)
    static TopicHandle<OrderEvent> topic;

    @Test
    void producePreservesKeyAndHeaders() {
        topic.produce(
            "order-key",
            Map.of("route", "priority", "trace", "t-1"),
            new OrderEvent("hdr-1", "alice", 10.0));

        List<ConsumedRecord<OrderEvent>> records = topic.awaitConsumedRecords(
            rs -> rs.stream().anyMatch(r -> "hdr-1".equals(r.value().orderId())),
            Duration.ofSeconds(20));

        ConsumedRecord<OrderEvent> record = records.stream()
            .filter(r -> "hdr-1".equals(r.value().orderId()))
            .findFirst()
            .orElseThrow();

        assertThat(record.key()).isEqualTo("order-key");
        assertThat(record.headers())
            .containsEntry("route", "priority")
            .containsEntry("trace", "t-1");
        assertThat(record.partition()).isBetween(0, 2);
    }

    @Test
    void awaitRecordsByKeyPreservesPerKeyOrder() {
        topic.produce("acct-a", new OrderEvent("k-1", "alice", 1.0));
        topic.produce("acct-b", new OrderEvent("k-2", "bob", 2.0));
        topic.produce("acct-a", new OrderEvent("k-3", "alice", 3.0));

        Map<String, List<OrderEvent>> byKey = topic.awaitRecordsByKey(
            groups -> groups.getOrDefault("acct-a", List.of()).size() >= 2
                && groups.getOrDefault("acct-b", List.of()).size() >= 1,
            Duration.ofSeconds(20));

        assertThat(byKey.get("acct-a"))
            .extracting(OrderEvent::orderId)
            .containsExactly("k-1", "k-3");
        assertThat(byKey.get("acct-b"))
            .extracting(OrderEvent::orderId)
            .containsExactly("k-2");
    }
}
