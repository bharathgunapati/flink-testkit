package io.flinktestkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

final class HttpEndpointPlugins {

    private static final Object LOCK = new Object();
    private static volatile HttpEndpointPlugin cached;
    private static volatile boolean resolved;

    private HttpEndpointPlugins() {
    }

    static Optional<HttpEndpointPlugin> find() {
        if (resolved) {
            return Optional.ofNullable(cached);
        }
        synchronized (LOCK) {
            if (resolved) {
                return Optional.ofNullable(cached);
            }
            List<HttpEndpointPlugin> found = new ArrayList<>();
            ServiceLoader.load(HttpEndpointPlugin.class).forEach(found::add);
            if (found.size() > 1) {
                throw new IllegalStateException(
                    "Multiple HttpEndpointPlugin implementations on the classpath: " + found);
            }
            cached = found.isEmpty() ? null : found.get(0);
            resolved = true;
            return Optional.ofNullable(cached);
        }
    }

    static HttpEndpointPlugin require() {
        return find().orElseThrow(() -> new IllegalStateException(
            "@HttpEndpoint requires the flink-testkit-http module on the test classpath. "
                + "Add:\n"
                + "  <dependency>\n"
                + "    <groupId>io.flinktestkit</groupId>\n"
                + "    <artifactId>flink-testkit-http</artifactId>\n"
                + "    <scope>test</scope>\n"
                + "  </dependency>"));
    }
}
