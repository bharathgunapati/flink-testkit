package io.flinktestkit;

/**
 * Optional SPI implemented by {@code flink-testkit-kafka-avro}.
 * Core stays free of Confluent/Avro dependencies; Format.AVRO loads this
 * via {@link java.util.ServiceLoader}.
 */
public interface AvroFormatPlugin {

    /**
     * Creates an Avro {@link ValueCodec} for the given SpecificRecord type.
     */
    <T> ValueCodec<T> createCodec(Class<T> valueType, String schemaRegistryUrl);

    /**
     * Ensures Schema Registry is running on the shared Testcontainers
     * network and returns its HTTP URL for clients on the host.
     */
    String ensureSchemaRegistryUrl(
        org.testcontainers.containers.Network network,
        String kafkaNetworkAlias);
}
