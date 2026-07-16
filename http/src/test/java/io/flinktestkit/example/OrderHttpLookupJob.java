package io.flinktestkit.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Example Flink job using Apache {@code flink-connector-http} lookup source
 * ({@code connector = 'http'}) to enrich orders via a temporal join.
 * Fixture for {@link OrderHttpLookupJobTest}, not part of the library.
 */
public final class OrderHttpLookupJob {

    private OrderHttpLookupJob() {
    }

    /**
     * Runs a bounded enrichment job and returns the joined rows.
     *
     * @param customersUrl full MockServer URL for the lookup endpoint
     * @param orders       input orders to enrich
     */
    public static List<Row> enrich(String customersUrl, OrderEvent... orders) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> orderStream = env.fromElements(toRows(orders))
            .returns(Types.ROW_NAMED(
                new String[] {"order_id", "customer_id", "amount"},
                Types.STRING,
                Types.STRING,
                Types.DOUBLE));

        Table ordersTable = tEnv.fromDataStream(
            orderStream,
            Schema.newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("customer_id", DataTypes.STRING())
                .column("amount", DataTypes.DOUBLE())
                .columnByExpression("proc_time", "PROCTIME()")
                .build());
        tEnv.createTemporaryView("orders", ordersTable);

        tEnv.executeSql("""
            CREATE TEMPORARY TABLE customers (
              customer_id STRING,
              tier STRING,
              region STRING
            ) WITH (
              'connector' = 'http',
              'format' = 'json',
              'url' = '%s',
              'asyncPolling' = 'true'
            )
            """.formatted(customersUrl));

        TableResult result = tEnv.executeSql("""
            SELECT o.order_id, o.customer_id, o.amount, c.tier, c.region
            FROM orders AS o
            JOIN customers FOR SYSTEM_TIME AS OF o.proc_time AS c
              ON o.customer_id = c.customer_id
            """);

        List<Row> rows = new ArrayList<>();
        CloseableIterator<Row> it = result.collect();
        try {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        } finally {
            try {
                it.close();
            } catch (IllegalStateException ignored) {
                // MiniCluster may already be shut down after a bounded job finishes.
            }
        }
        return rows;
    }

    private static Row[] toRows(OrderEvent... orders) {
        Row[] rows = new Row[orders.length];
        for (int i = 0; i < orders.length; i++) {
            OrderEvent order = orders[i];
            rows[i] = Row.of(order.orderId(), order.customerName(), order.amount());
        }
        return rows;
    }
}
