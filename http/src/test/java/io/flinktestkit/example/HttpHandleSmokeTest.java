package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.HttpEndpoint;
import io.flinktestkit.HttpHandle;
import io.flinktestkit.ReceivedRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HTTP-only smoke test: stub + await without a Flink job.
 */
@ExtendWith(FlinkTestExtension.class)
class HttpHandleSmokeTest {

    @HttpEndpoint(path = "/ping")
    static HttpHandle ping;

    @Test
    void stubAndAwaitRequests() throws Exception {
        ping.stubPost(200);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(ping.url()))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"ok\":true}"))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);

        List<ReceivedRequest> hits = ping.awaitRequests(1, Duration.ofSeconds(5));
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).body()).contains("ok");
    }
}
