package io.flinktestkit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * SPI implementation: shared Kafka lifecycle + {@link KafkaTopic} injection.
 */
public final class KafkaTopicPluginImpl implements KafkaTopicPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicPluginImpl.class);

    static final String KAFKA_ALIAS = "kafka";
    static final Network NETWORK = Network.newNetwork();

    private static final Object KAFKA_LOCK = new Object();
    private static volatile KafkaContainer kafka;

    private final ThreadLocal<List<TopicHandle<?>>> handlesForCurrentClass =
        ThreadLocal.withInitial(ArrayList::new);

    @Override
    public void beforeAll(Class<?> testClass) throws Exception {
        List<TopicHandle<?>> createdHandles = new ArrayList<>();

        for (Field field : testClass.getDeclaredFields()) {
            KafkaTopic annotation = field.getAnnotation(KafkaTopic.class);
            if (annotation == null) {
                continue;
            }
            validateField(field);

            TopicHandle<?> handle = createTopicHandle(field, annotation);
            createdHandles.add(handle);

            field.setAccessible(true);
            field.set(null, handle);
        }

        handlesForCurrentClass.set(createdHandles);
    }

    @Override
    public void afterAll(Class<?> testClass) throws Exception {
        for (TopicHandle<?> handle : handlesForCurrentClass.get()) {
            handle.close();
            deleteTopicQuietly(handle.topicName());
            if (handle.hasDlq()) {
                deleteTopicQuietly(handle.dlqTopicName());
            }
        }
        handlesForCurrentClass.remove();
    }

    @Override
    public String bootstrapServers() {
        return ensureKafka().getBootstrapServers();
    }

    @Override
    public String schemaRegistryUrl() {
        ensureKafka();
        return AvroFormatPlugins.require().ensureSchemaRegistryUrl(NETWORK, KAFKA_ALIAS);
    }

    private void validateField(Field field) {
        if (!Modifier.isStatic(field.getModifiers())) {
            throw new IllegalStateException(
                "@KafkaTopic field '" + field.getName() + "' on " + field.getDeclaringClass().getSimpleName()
                    + " must be static, so it can be populated in beforeAll() and shared "
                    + "across every @Test method in the class.");
        }
        if (!TopicHandle.class.equals(field.getType())) {
            throw new IllegalStateException(
                "@KafkaTopic field '" + field.getName() + "' on " + field.getDeclaringClass().getSimpleName()
                    + " must be of type TopicHandle<T>, matching the annotation's valueType.");
        }
    }

    private TopicHandle<?> createTopicHandle(Field field, KafkaTopic annotation) throws Exception {
        ensureKafka();

        String prefix = annotation.name().isBlank() ? field.getName() : annotation.name();
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topicName = prefix + "-" + suffix;

        createTopicOnBroker(topicName, annotation.partitions());

        Format format = annotation.format();
        String registryUrl = null;
        if (format == Format.AVRO) {
            registryUrl = schemaRegistryUrl();
        }

        DlqHandle dlq = null;
        if (annotation.dlq()) {
            String dlqTopicName = prefix + "-dlq-" + suffix;
            createTopicOnBroker(dlqTopicName, annotation.partitions());
            dlq = new DlqHandle(dlqTopicName, kafka.getBootstrapServers());
            LOG.debug("Created DLQ companion topic '{}'", dlqTopicName);
        }

        return new TopicHandle<>(
            topicName,
            annotation.valueType(),
            kafka.getBootstrapServers(),
            format,
            registryUrl,
            dlq);
    }

    private static KafkaContainer ensureKafka() {
        KafkaContainer local = kafka;
        if (local != null && local.isRunning()) {
            return local;
        }
        synchronized (KAFKA_LOCK) {
            if (kafka != null && kafka.isRunning()) {
                return kafka;
            }
            KafkaContainer container = new KafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                .withNetwork(NETWORK)
                .withNetworkAliases(KAFKA_ALIAS);
            container.start();
            kafka = container;
            Runtime.getRuntime().addShutdownHook(
                new Thread(container::stop, "flink-testkit-kafka-shutdown"));
            LOG.info("Shared Kafka container started at {}", container.getBootstrapServers());
            return container;
        }
    }

    private void createTopicOnBroker(String topicName, int partitions) throws Exception {
        try (AdminClient admin = adminClient()) {
            List<NewTopic> topics = List.of(new NewTopic(topicName, partitions, (short) 1));
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            LOG.debug("Created topic '{}' with {} partition(s)", topicName, partitions);
        }
    }

    private void deleteTopicQuietly(String topicName) {
        try (AdminClient admin = adminClient()) {
            admin.deleteTopics(List.of(topicName)).all().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.debug("Could not delete topic '{}' during teardown: {}", topicName, e.toString());
        }
    }

    private AdminClient adminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ensureKafka().getBootstrapServers());
        return AdminClient.create(props);
    }
}
