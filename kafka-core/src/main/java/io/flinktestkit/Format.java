package io.flinktestkit;

/**
 * Serialization format for a {@link KafkaTopic}'s record values.
 *
 * <p>{@link #JSON} is built into {@code flink-testkit-kafka-core}.
 * {@link #AVRO} requires the optional {@code flink-testkit-kafka-avro}
 * module on the test classpath; Schema Registry starts on first use.
 */
public enum Format {
    JSON,
    AVRO
}
