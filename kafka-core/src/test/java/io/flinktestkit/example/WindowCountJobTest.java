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

@ExtendWith(FlinkTestExtension.class)
class WindowCountJobTest {

    @KafkaTopic(valueType = ClickEvent.class)
    static TopicHandle<ClickEvent> clicks;

    @KafkaTopic(valueType = WindowCount.class)
    static TopicHandle<WindowCount> counts;

    @Test
    void countsClicksInTumblingEventTimeWindows() throws Exception {
        JobClient job = WindowCountJob.runAsync(
            clicks.bootstrapServers(), clicks.topicName(), counts.topicName());

        try {
            long windowStart = 1_700_000_000_000L; // aligned enough; window size is 5s
            // Two clicks in [windowStart, windowStart+5s), one in the next window.
            // A later event advances the watermark so the first window can close.
            clicks.produce(
                new ClickEvent("u1", windowStart + 1_000),
                new ClickEvent("u1", windowStart + 2_000),
                new ClickEvent("u1", windowStart + 6_000),
                // watermark advance past first window end (+1s out-of-orderness)
                new ClickEvent("u1", windowStart + 12_000));

            List<WindowCount> results = counts.awaitRecords(
                records -> records.stream().anyMatch(r ->
                    "u1".equals(r.userId()) && r.count() == 2),
                Duration.ofSeconds(45));

            assertThat(results)
                .filteredOn(r -> r.count() == 2)
                .extracting(WindowCount::userId)
                .contains("u1");
        } finally {
            job.cancel();
        }
    }
}
