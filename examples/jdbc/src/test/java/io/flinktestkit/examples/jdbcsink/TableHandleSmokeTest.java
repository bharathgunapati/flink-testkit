package io.flinktestkit.examples.jdbcsink;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.JdbcTable;
import io.flinktestkit.TableHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JDBC-only consumer demo: seed with {@link TableHandle#insert} and assert
 * with {@link TableHandle#awaitRows}, without a Flink job.
 */
@ExtendWith(FlinkTestExtension.class)
class TableHandleSmokeTest {

    @JdbcTable(
        valueType = OrderRow.class,
        ddl = """
            CREATE TABLE {table} (
              order_id VARCHAR(64) PRIMARY KEY,
              customer_name VARCHAR(128) NOT NULL,
              amount DOUBLE PRECISION NOT NULL
            )
            """)
    static TableHandle<OrderRow> orders;

    @Test
    void insertAndAwaitRows() {
        orders.insert(
            new OrderRow("o-1", "bob", 10.0),
            new OrderRow("o-2", "carol", 20.0));

        List<OrderRow> rows = orders.awaitRows(2, Duration.ofSeconds(5));

        assertThat(rows)
            .extracting(OrderRow::orderId)
            .containsExactlyInAnyOrder("o-1", "o-2");
    }
}
