package io.flinktestkit;

/**
 * Optional SPI implemented by {@code flink-testkit-kafka-core}.
 * Core stays free of Kafka/Testcontainers dependencies; {@link FlinkTestExtension}
 * loads this via {@link java.util.ServiceLoader} when present.
 */
public interface KafkaTopicPlugin {

    /**
     * Scans {@code testClass} for {@code @KafkaTopic} fields, starts Kafka
     * lazily if needed, creates topics, and injects typed handles.
     */
    void beforeAll(Class<?> testClass) throws Exception;

    /**
     * Deletes topics created for {@code testClass} and closes per-class resources.
     */
    void afterAll(Class<?> testClass) throws Exception;

    /** Bootstrap servers of the shared Kafka container (starts Kafka on first call). */
    String bootstrapServers();

    /**
     * Schema Registry URL. Requires {@code flink-testkit-kafka-avro}.
     * Starts Schema Registry on first call.
     */
    String schemaRegistryUrl();
}
