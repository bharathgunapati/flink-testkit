package io.flinktestkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

final class JdbcTablePlugins {

    private static final Object LOCK = new Object();
    private static volatile JdbcTablePlugin cached;
    private static volatile boolean resolved;

    private JdbcTablePlugins() {
    }

    static Optional<JdbcTablePlugin> find() {
        if (resolved) {
            return Optional.ofNullable(cached);
        }
        synchronized (LOCK) {
            if (resolved) {
                return Optional.ofNullable(cached);
            }
            List<JdbcTablePlugin> found = new ArrayList<>();
            ServiceLoader.load(JdbcTablePlugin.class).forEach(found::add);
            if (found.size() > 1) {
                throw new IllegalStateException(
                    "Multiple JdbcTablePlugin implementations on the classpath: " + found);
            }
            cached = found.isEmpty() ? null : found.get(0);
            resolved = true;
            return Optional.ofNullable(cached);
        }
    }

    static JdbcTablePlugin require() {
        return find().orElseThrow(() -> new IllegalStateException(
            "@JdbcTable requires the flink-testkit-jdbc module on the test classpath. "
                + "Add:\n"
                + "  <dependency>\n"
                + "    <groupId>io.flinktestkit</groupId>\n"
                + "    <artifactId>flink-testkit-jdbc</artifactId>\n"
                + "    <scope>test</scope>\n"
                + "  </dependency>"));
    }
}
