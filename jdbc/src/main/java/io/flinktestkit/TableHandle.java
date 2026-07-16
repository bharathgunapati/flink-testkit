package io.flinktestkit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A typed handle onto a single, test-isolated Postgres table.
 *
 * <p>Created and injected by {@link FlinkTestExtension} via the JDBC plugin —
 * test code should never construct this directly, only declare it via
 * {@link JdbcTable}.
 *
 * <p>Use {@link #insert} to seed rows, {@link #awaitRows} to wait for the
 * job under test to write outcomes, and {@link #jdbcUrl()} /
 * {@link #tableName()} (plus credentials) to configure the Flink JDBC
 * connector under test.
 *
 * @param <T> the Java type each row maps to
 */
public final class TableHandle<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
        new TypeReference<>() {
        };

    private final String tableName;
    private final Class<T> valueType;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String driverName;

    TableHandle(
            String tableName,
            Class<T> valueType,
            String jdbcUrl,
            String username,
            String password,
            String driverName) {
        this.tableName = tableName;
        this.valueType = valueType;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.driverName = driverName;
    }

    /** The actual (randomized) table name created for this test. */
    public String tableName() {
        return tableName;
    }

    /** JDBC URL of the shared Postgres container. */
    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    /** JDBC driver class name (Postgres). */
    public String driverName() {
        return driverName;
    }

    /**
     * Inserts each row into this table, mapping camelCase properties to
     * snake_case columns. Blocks until the inserts commit.
     */
    @SafeVarargs
    public final void insert(T... rows) {
        if (rows == null || rows.length == 0) {
            throw new IllegalArgumentException("At least one row is required");
        }
        try (Connection connection = openConnection()) {
            for (T row : rows) {
                insertOne(connection, row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert into table '" + tableName + "'", e);
        }
    }

    /**
     * Blocks until at least {@code expectedCount} rows are present in this
     * table, then returns them mapped to {@code T}.
     */
    public List<T> awaitRows(int expectedCount, Duration timeout) {
        if (expectedCount < 0) {
            throw new IllegalArgumentException("expectedCount must be >= 0");
        }
        return awaitRows(rows -> rows.size() >= expectedCount, timeout);
    }

    /**
     * Blocks until the accumulated rows satisfy {@code condition}, polling
     * on a real completion condition instead of a fixed sleep.
     */
    public List<T> awaitRows(Predicate<List<T>> condition, Duration timeout) {
        if (condition == null) {
            throw new IllegalArgumentException("condition is required");
        }
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }

        Instant deadline = Instant.now().plus(timeout);
        try {
            while (true) {
                List<T> rows = selectAll();
                if (condition.test(rows)) {
                    return List.copyOf(rows);
                }
                if (Instant.now().isAfter(deadline)) {
                    throw new AssertionError(
                        "Timed out after " + timeout + " waiting for rows in table '"
                            + tableName + "'. Last seen " + rows.size() + " row(s): " + rows);
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while awaiting rows in '" + tableName + "'", e);
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed while awaiting rows in '" + tableName + "'", e);
        }
    }

    /** Returns every row currently in the table, mapped to {@code T}. */
    public List<T> selectAll() {
        String sql = "SELECT * FROM " + quotedTable();
        try (Connection connection = openConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return mapResultSet(resultSet);
        } catch (Exception e) {
            throw new RuntimeException("Failed to select from table '" + tableName + "'", e);
        }
    }

    void close() {
        // no pooled resources yet; tables are dropped by the plugin
    }

    private void insertOne(Connection connection, T row) throws SQLException {
        Map<String, Object> columns = MAPPER.convertValue(row, MAP_TYPE);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(
                "valueType " + valueType.getName() + " produced no columns to insert");
        }

        List<String> names = new ArrayList<>(columns.keySet());
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(quotedTable()).append(" (");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdent(names.get(i)));
        }
        sql.append(") VALUES (");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append('?');
        }
        sql.append(')');

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < names.size(); i++) {
                statement.setObject(i + 1, columns.get(names.get(i)));
            }
            statement.executeUpdate();
        }
    }

    private List<T> mapResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData meta = resultSet.getMetaData();
        int columnCount = meta.getColumnCount();
        List<T> rows = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> values = new LinkedHashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                values.put(meta.getColumnLabel(i).toLowerCase(), resultSet.getObject(i));
            }
            rows.add(MAPPER.convertValue(values, valueType));
        }
        return rows;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    private String quotedTable() {
        return quoteIdent(tableName);
    }

    private static String quoteIdent(String ident) {
        return '"' + ident.replace("\"", "\"\"") + '"';
    }
}
