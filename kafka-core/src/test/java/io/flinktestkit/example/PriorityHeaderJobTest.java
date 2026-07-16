package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(FlinkTestExtension.class)
class PriorityHeaderJobTest {

    @KafkaTopic(valueType = OrderEvent.class)
    static TopicHandle<OrderEvent> input;

    @KafkaTopic(valueType = OrderEvent.class)
    static TopicHandle<OrderEvent> output;

    @Test
    void forwardsOnlyPriorityRoutedRecords() throws Exception {
        JobClient job = PriorityHeaderJob.runAsync(
            input.bootstrapServers(), input.topicName(), output.topicName());

        try {
            input.produce("k", Map.of("route", "batch"), new OrderEvent("p-1", "alice", 1.0));
            input.produce("k", Map.of("route", "priority"), new OrderEvent("p-2", "bob", 2.0));
            input.produce(new OrderEvent("p-3", "carol", 3.0));

            List<OrderEvent> results = output.awaitRecords(
                records -> records.stream().anyMatch(r -> "p-2".equals(r.orderId())),
                Duration.ofSeconds(45));

            assertThat(results)
                .extracting(OrderEvent::orderId)
                .contains("p-2")
                .doesNotContain("p-1", "p-3");
        } finally {
            job.cancel();
        }
    }
}
