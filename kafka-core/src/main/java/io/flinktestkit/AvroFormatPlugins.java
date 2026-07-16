package io.flinktestkit;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

final class AvroFormatPlugins {

    private static final Object LOCK = new Object();
    private static volatile AvroFormatPlugin cached;

    private AvroFormatPlugins() {
    }

    static AvroFormatPlugin require() {
        AvroFormatPlugin plugin = cached;
        if (plugin != null) {
            return plugin;
        }
        synchronized (LOCK) {
            if (cached != null) {
                return cached;
            }
            List<AvroFormatPlugin> found = new ArrayList<>();
            ServiceLoader.load(AvroFormatPlugin.class).forEach(found::add);
            if (found.isEmpty()) {
                throw new IllegalStateException(
                    "Format.AVRO requires the flink-testkit-kafka-avro module on the test classpath. "
                        + "Add:\n"
                        + "  <dependency>\n"
                        + "    <groupId>io.flinktestkit</groupId>\n"
                        + "    <artifactId>flink-testkit-kafka-avro</artifactId>\n"
                        + "    <scope>test</scope>\n"
                        + "  </dependency>");
            }
            if (found.size() > 1) {
                throw new IllegalStateException(
                    "Multiple AvroFormatPlugin implementations on the classpath: " + found);
            }
            cached = found.get(0);
            return cached;
        }
    }
}
