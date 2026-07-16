package io.flinktestkit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * SPI implementation: shared Postgres lifecycle + {@link JdbcTable} injection.
 */
public final class JdbcTablePluginImpl implements JdbcTablePlugin {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcTablePluginImpl.class);
    private static final String DRIVER = "org.postgresql.Driver";
    private static final Object POSTGRES_LOCK = new Object();
    private static volatile PostgreSQLContainer<?> postgres;

    private final ThreadLocal<List<TableHandle<?>>> handlesForCurrentClass =
        ThreadLocal.withInitial(ArrayList::new);

    @Override
    public void beforeAll(Class<?> testClass) throws Exception {
        List<TableHandle<?>> created = new ArrayList<>();

        for (Field field : testClass.getDeclaredFields()) {
            JdbcTable annotation = field.getAnnotation(JdbcTable.class);
            if (annotation == null) {
                continue;
            }
            validateField(field);

            TableHandle<?> handle = createTableHandle(field, annotation);
            created.add(handle);

            field.setAccessible(true);
            field.set(null, handle);
        }

        handlesForCurrentClass.set(created);
    }

    @Override
    public void afterAll(Class<?> testClass) throws Exception {
        PostgreSQLContainer<?> container = postgres;
        for (TableHandle<?> handle : handlesForCurrentClass.get()) {
            handle.close();
            if (container != null && container.isRunning()) {
                dropTableQuietly(handle.tableName());
            }
        }
        handlesForCurrentClass.remove();
    }

    private void validateField(Field field) {
        if (!Modifier.isStatic(field.getModifiers())) {
            throw new IllegalStateException(
                "@JdbcTable field '" + field.getName() + "' on " + field.getDeclaringClass().getSimpleName()
                    + " must be static, so it can be populated in beforeAll() and shared "
                    + "across every @Test method in the class.");
        }
        if (!TableHandle.class.equals(field.getType())) {
            throw new IllegalStateException(
                "@JdbcTable field '" + field.getName() + "' on " + field.getDeclaringClass().getSimpleName()
                    + " must be of type TableHandle<T>, matching the annotation's valueType.");
        }
    }

    private TableHandle<?> createTableHandle(Field field, JdbcTable annotation) throws Exception {
        PostgreSQLContainer<?> container = ensurePostgres();

        String prefix = annotation.name().isBlank() ? field.getName() : annotation.name();
        String suffix = UUID.randomUUID().toString().substring(0, 8).replace('-', '_');
        String tableName = sanitizeIdent(prefix + "_" + suffix);

        String ddl = annotation.ddl();
        if (ddl == null || ddl.isBlank()) {
            throw new IllegalStateException(
                "@JdbcTable field '" + field.getName() + "' requires a non-blank ddl() "
                    + "with a {table} placeholder.");
        }
        if (!ddl.contains("{table}")) {
            throw new IllegalStateException(
                "@JdbcTable field '" + field.getName() + "' ddl() must contain the {table} "
                    + "placeholder for the generated table name.");
        }

        String renderedDdl = ddl.replace("{table}", quoteIdent(tableName));
        try (Connection connection = DriverManager.getConnection(
                 container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute(renderedDdl);
        }
        LOG.debug("Created Postgres table '{}'", tableName);

        return new TableHandle<>(
            tableName,
            annotation.valueType(),
            container.getJdbcUrl(),
            container.getUsername(),
            container.getPassword(),
            DRIVER);
    }

    private static PostgreSQLContainer<?> ensurePostgres() {
        PostgreSQLContainer<?> local = postgres;
        if (local != null && local.isRunning()) {
            return local;
        }
        synchronized (POSTGRES_LOCK) {
            if (postgres != null && postgres.isRunning()) {
                return postgres;
            }
            @SuppressWarnings("resource")
            PostgreSQLContainer<?> container = new PostgreSQLContainer<>(
                DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("flink_testkit")
                .withUsername("test")
                .withPassword("test");
            container.start();
            postgres = container;
            Runtime.getRuntime().addShutdownHook(
                new Thread(container::stop, "flink-testkit-postgres-shutdown"));
            LOG.info("Shared Postgres container started at {}", container.getJdbcUrl());
            return container;
        }
    }

    private void dropTableQuietly(String tableName) {
        try (Connection connection = DriverManager.getConnection(
                 postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + quoteIdent(tableName));
        } catch (Exception e) {
            LOG.debug("Could not drop table '{}' during teardown: {}", tableName, e.toString());
        }
    }

    private static String sanitizeIdent(String raw) {
        String cleaned = raw.replaceAll("[^A-Za-z0-9_]", "_");
        if (cleaned.isEmpty() || Character.isDigit(cleaned.charAt(0))) {
            cleaned = "t_" + cleaned;
        }
        return cleaned.toLowerCase();
    }

    private static String quoteIdent(String ident) {
        return '"' + ident.replace("\"", "\"\"") + '"';
    }
}
