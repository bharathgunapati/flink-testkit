package io.flinktestkit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a Postgres table that a test needs, and the Java type each row
 * should be mapped as.
 *
 * <p>Annotate a {@code static} field of type {@link TableHandle} on a test
 * class extended with {@link FlinkTestExtension}. Requires
 * {@code flink-testkit-jdbc} on the test classpath. The extension creates a
 * uniquely-named table on a shared Postgres container, runs {@link #ddl()},
 * and injects a ready {@link TableHandle} before any {@code @Test} runs.
 *
 * <p>Use {@code {table}} in {@link #ddl()} as the placeholder for the
 * generated table name (quoted as an identifier).
 *
 * <pre>{@code
 * @ExtendWith(FlinkTestExtension.class)
 * class MyJobTest {
 *
 *     @JdbcTable(
 *         valueType = Order.class,
 *         ddl = """
 *             CREATE TABLE {table} (
 *               order_id VARCHAR(64) PRIMARY KEY,
 *               customer_name VARCHAR(128) NOT NULL,
 *               amount DOUBLE PRECISION NOT NULL
 *             )
 *             """)
 *     static TableHandle<Order> orders;
 * }
 * }</pre>
 *
 * <p>Column names are expected in {@code snake_case}, matching Java record
 * / bean property names converted from camelCase (e.g. {@code orderId} →
 * {@code order_id}).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JdbcTable {

    /** The Java type each row should be mapped as. */
    Class<?> valueType();

    /**
     * DDL executed once per test class after the table name is chosen.
     * Must contain the {@code {table}} placeholder for the generated name.
     */
    String ddl();

    /**
     * Optional name prefix for readability in logs. A random suffix is
     * always appended so tests stay isolated.
     */
    String name() default "";
}
