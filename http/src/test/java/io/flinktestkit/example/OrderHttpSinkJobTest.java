package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.HttpEndpoint;
import io.flinktestkit.HttpHandle;
import io.flinktestkit.ReceivedRequest;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end demo: Apache Flink {@code HttpSink} → MockServer via {@link HttpHandle}.
 */
@ExtendWith(FlinkTestExtension.class)
class OrderHttpSinkJobTest {

    @HttpEndpoint(path = "/ingest")
    static HttpHandle ingest;

    @Test
    void postsOrdersToHttpEndpoint() throws Exception {
        ingest.stubPost(202);

        JobClient jobClient = OrderHttpSinkJob.runAsync(
            ingest.url(),
            new OrderEvent("order-1", "alice", 42.50));

        try {
            List<ReceivedRequest> hits = ingest.awaitRequests(
                reqs -> reqs.stream().anyMatch(r -> r.body().contains("order-1")),
                Duration.ofSeconds(45));

            ReceivedRequest hit = hits.stream()
                .filter(r -> r.body().contains("order-1"))
                .findFirst()
                .orElseThrow();
            assertThat(hit.method()).isEqualTo("POST");
            assertThat(hit.path()).isEqualTo(ingest.path());
            assertThat(hit.body()).contains("alice");
        } finally {
            try {
                jobClient.cancel().get();
            } catch (Exception ignored) {
                // Job may already have finished for a bounded source.
            }
        }
    }
}
