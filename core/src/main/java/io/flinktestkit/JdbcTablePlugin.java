package io.flinktestkit;

/**
 * Optional SPI implemented by {@code flink-testkit-jdbc}.
 * Core stays free of Postgres/JDBC driver dependencies; {@link FlinkTestExtension}
 * loads this via {@link java.util.ServiceLoader} when present.
 */
public interface JdbcTablePlugin {

    /**
     * Scans {@code testClass} for {@code @JdbcTable} fields, starts Postgres
     * lazily if needed, creates tables, and injects typed handles.
     */
    void beforeAll(Class<?> testClass) throws Exception;

    /**
     * Drops tables created for {@code testClass} and closes per-class resources.
     */
    void afterAll(Class<?> testClass) throws Exception;
}
