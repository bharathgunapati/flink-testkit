package io.flinktestkit.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example Flink job: Kafka JSON source → JDBC (Postgres) sink.
 * Fixture for {@link OrderJdbcSinkJobTest}, not part of the library.
 */
public final class OrderJdbcSinkJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OrderJdbcSinkJob() {
    }

    public static JobClient runAsync(
            String bootstrapServers,
            String inputTopic,
            String jdbcUrl,
            String username,
            String password,
            String tableName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("order-jdbc-sink-job")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<OrderRow> orders = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "orders-source")
            .map(json -> MAPPER.readValue(json, OrderRow.class));

        String insertSql = "INSERT INTO \"" + tableName + "\" "
            + "(order_id, customer_name, amount) VALUES (?, ?, ?)";

        orders.addSink(JdbcSink.sink(
            insertSql,
            (statement, order) -> {
                statement.setString(1, order.orderId());
                statement.setString(2, order.customerName());
                statement.setDouble(3, order.amount());
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1)
                .withBatchIntervalMs(100)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build()));

        return env.executeAsync("order-jdbc-sink-job");
    }
}
