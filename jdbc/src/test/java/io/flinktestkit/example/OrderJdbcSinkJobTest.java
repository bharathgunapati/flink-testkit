package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.JdbcTable;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TableHandle;
import io.flinktestkit.TopicHandle;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end demo: Kafka {@link TopicHandle} + Postgres {@link TableHandle}
 * composed through a single {@link FlinkTestExtension}.
 */
@ExtendWith(FlinkTestExtension.class)
class OrderJdbcSinkJobTest {

    @KafkaTopic(valueType = OrderRow.class)
    static TopicHandle<OrderRow> orders;

    @JdbcTable(
        valueType = OrderRow.class,
        ddl = """
            CREATE TABLE {table} (
              order_id VARCHAR(64) PRIMARY KEY,
              customer_name VARCHAR(128) NOT NULL,
              amount DOUBLE PRECISION NOT NULL
            )
            """)
    static TableHandle<OrderRow> orderRows;

    @Test
    void writesOrdersFromKafkaToPostgres() throws Exception {
        JobClient jobClient = OrderJdbcSinkJob.runAsync(
            orders.bootstrapServers(),
            orders.topicName(),
            orderRows.jdbcUrl(),
            orderRows.username(),
            orderRows.password(),
            orderRows.tableName());

        try {
            orders.produce(new OrderRow("order-1", "alice", 42.50));

            List<OrderRow> rows = orderRows.awaitRows(
                found -> found.stream().anyMatch(r -> "order-1".equals(r.orderId())),
                Duration.ofSeconds(45));

            OrderRow row = rows.stream()
                .filter(r -> "order-1".equals(r.orderId()))
                .findFirst()
                .orElseThrow();
            assertThat(row.customerName()).isEqualTo("alice");
            assertThat(row.amount()).isEqualTo(42.50);
        } finally {
            jobClient.cancel();
        }
    }
}
