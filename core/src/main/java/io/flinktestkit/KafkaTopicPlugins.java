package io.flinktestkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

final class KafkaTopicPlugins {

    private static final Object LOCK = new Object();
    private static volatile KafkaTopicPlugin cached;
    private static volatile boolean resolved;

    private KafkaTopicPlugins() {
    }

    static Optional<KafkaTopicPlugin> find() {
        if (resolved) {
            return Optional.ofNullable(cached);
        }
        synchronized (LOCK) {
            if (resolved) {
                return Optional.ofNullable(cached);
            }
            List<KafkaTopicPlugin> found = new ArrayList<>();
            ServiceLoader.load(KafkaTopicPlugin.class).forEach(found::add);
            if (found.size() > 1) {
                throw new IllegalStateException(
                    "Multiple KafkaTopicPlugin implementations on the classpath: " + found);
            }
            cached = found.isEmpty() ? null : found.get(0);
            resolved = true;
            return Optional.ofNullable(cached);
        }
    }

    static KafkaTopicPlugin require() {
        return find().orElseThrow(() -> new IllegalStateException(
            "@KafkaTopic requires the flink-testkit-kafka-core module on the test classpath. "
                + "Add:\n"
                + "  <dependency>\n"
                + "    <groupId>io.flinktestkit</groupId>\n"
                + "    <artifactId>flink-testkit-kafka-core</artifactId>\n"
                + "    <scope>test</scope>\n"
                + "  </dependency>"));
    }
}
