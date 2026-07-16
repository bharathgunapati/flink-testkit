package io.flinktestkit.examples.httplookup;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.HttpEndpoint;
import io.flinktestkit.HttpHandle;
import io.flinktestkit.ReceivedRequest;
import io.flinktestkit.examples.httpsink.OrderEvent;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer-style HTTP lookup demo: temporal join enrichment via MockServer.
 */
@ExtendWith(FlinkTestExtension.class)
class OrderHttpLookupJobTest {

    @HttpEndpoint(path = "/customers")
    static HttpHandle customers;

    @Test
    void enrichesOrdersViaHttpLookup() throws Exception {
        customers.stubGetJson(
            200,
            """
            {"customer_id":"alice","tier":"gold","region":"us-west"}
            """);

        List<Row> enriched = OrderHttpLookupJob.enrich(
            customers.url(),
            new OrderEvent("order-1", "alice", 42.50));

        assertThat(enriched).hasSize(1);
        Row row = enriched.get(0);
        assertThat(row.getField(0)).isEqualTo("order-1");
        assertThat(row.getField(1)).isEqualTo("alice");
        assertThat(row.getField(2)).isEqualTo(42.50);
        assertThat(row.getField(3)).isEqualTo("gold");
        assertThat(row.getField(4)).isEqualTo("us-west");

        List<ReceivedRequest> hits = customers.awaitRequests(1, Duration.ofSeconds(5));
        ReceivedRequest hit = hits.get(0);
        assertThat(hit.method()).isEqualTo("GET");
        assertThat(hit.path()).isEqualTo(customers.path());
        assertThat(hit.queryParams()).containsEntry("customer_id", List.of("alice"));
    }
}
