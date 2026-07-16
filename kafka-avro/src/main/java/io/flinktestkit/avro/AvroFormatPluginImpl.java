package io.flinktestkit.avro;

import io.flinktestkit.AvroFormatPlugin;
import io.flinktestkit.ValueCodec;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * ServiceLoader entry for Avro + Schema Registry support.
 */
public final class AvroFormatPluginImpl implements AvroFormatPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(AvroFormatPluginImpl.class);
    private static final Object LOCK = new Object();
    private static volatile GenericContainer<?> schemaRegistry;

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> ValueCodec<T> createCodec(Class<T> valueType, String schemaRegistryUrl) {
        if (!SpecificRecord.class.isAssignableFrom(valueType)) {
            throw new IllegalArgumentException(
                "@KafkaTopic format=AVRO requires valueType to implement "
                    + "org.apache.avro.specific.SpecificRecord (usually an avro-maven-plugin "
                    + "generated class). Got: " + valueType.getName());
        }
        return (ValueCodec<T>) new AvroValueCodec<>((Class) valueType, schemaRegistryUrl);
    }

    @Override
    public String ensureSchemaRegistryUrl(Network network, String kafkaNetworkAlias) {
        GenericContainer<?> local = schemaRegistry;
        if (local != null && local.isRunning()) {
            return url(local);
        }
        synchronized (LOCK) {
            if (schemaRegistry != null && schemaRegistry.isRunning()) {
                return url(schemaRegistry);
            }
            GenericContainer<?> registry = new GenericContainer<>(
                DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv(
                    "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    "PLAINTEXT://" + kafkaNetworkAlias + ":9092")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
            registry.start();
            schemaRegistry = registry;
            Runtime.getRuntime().addShutdownHook(
                new Thread(registry::stop, "flink-testkit-schema-registry-shutdown"));
            String registryUrl = url(registry);
            LOG.info("Shared Schema Registry started at {}", registryUrl);
            return registryUrl;
        }
    }

    private static String url(GenericContainer<?> registry) {
        return "http://" + registry.getHost() + ":" + registry.getMappedPort(8081);
    }
}
