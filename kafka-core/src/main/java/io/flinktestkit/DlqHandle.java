package io.flinktestkit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Handle onto a DLQ companion topic created when {@link KafkaTopic#dlq()} is
 * {@code true}. Values are raw bytes — poison payloads are not expected to
 * deserialize as the parent topic's value type.
 */
public final class DlqHandle {

    private final String topicName;
    private final String bootstrapServers;

    DlqHandle(String topicName, String bootstrapServers) {
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
    }

    /** Randomized DLQ topic name on the shared broker. */
    public String topicName() {
        return topicName;
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Blocks until at least {@code expectedCount} raw payloads arrive on the
     * DLQ, then returns them.
     */
    public List<byte[]> awaitRecords(int expectedCount, Duration timeout) {
        return awaitConsumedRecords(expectedCount, timeout).stream()
            .map(ConsumedRecord::value)
            .toList();
    }

    /**
     * Blocks until the accumulated DLQ {@link ConsumedRecord}s satisfy
     * {@code condition}.
     */
    public List<ConsumedRecord<byte[]>> awaitConsumedRecords(
            Predicate<List<ConsumedRecord<byte[]>>> condition, Duration timeout) {
        List<ConsumedRecord<byte[]>> collected = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-testkit-dlq-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topicName));
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, byte[]> record : records) {
                    collected.add(toConsumedRecord(record));
                }
                if (condition.test(collected)) {
                    return List.copyOf(collected);
                }
            }
        }

        throw new AssertionError(
            "Timed out after " + timeout + " waiting for DLQ records on topic '" + topicName
                + "'. Collected " + collected.size() + " record(s) so far");
    }

    public List<ConsumedRecord<byte[]>> awaitConsumedRecords(int expectedCount, Duration timeout) {
        return awaitConsumedRecords(records -> records.size() >= expectedCount, timeout);
    }

    private static ConsumedRecord<byte[]> toConsumedRecord(ConsumerRecord<String, byte[]> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            if (header.value() == null) {
                headers.put(header.key(), null);
            } else {
                headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        byte[] value = record.value() == null ? new byte[0] : record.value().clone();
        return new ConsumedRecord<>(record.key(), value, headers, record.partition(), record.offset());
    }
}
