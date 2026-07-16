package io.flinktestkit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Predicate;

/**
 * A typed handle onto a single, test-isolated Kafka topic.
 *
 * <p>Created and injected by {@link FlinkTestExtension} via the Kafka plugin —
 * test code should never construct this directly, only declare it via
 * {@link KafkaTopic}.
 *
 * <p>Use {@link #produce} to push typed records, {@link #produceInvalid} to
 * inject poison payloads, and {@link #awaitRecords} / {@link #awaitDlqRecords}
 * to wait for outcomes. Await helpers poll on a real completion condition
 * instead of a fixed sleep.
 *
 * @param <T> the Java type each record's value is (de)serialized as
 */
public final class TopicHandle<T> {

    private final String topicName;
    private final Class<T> valueType;
    private final String bootstrapServers;
    private final Format format;
    private final String schemaRegistryUrl;
    private final ValueCodec<T> codec;
    private final DlqHandle dlq;

    private volatile KafkaProducer<String, byte[]> producer;

    TopicHandle(
            String topicName,
            Class<T> valueType,
            String bootstrapServers,
            Format format,
            String schemaRegistryUrl,
            DlqHandle dlq) {
        this.topicName = topicName;
        this.valueType = valueType;
        this.bootstrapServers = bootstrapServers;
        this.format = format;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.codec = ValueCodecs.create(format, valueType, schemaRegistryUrl);
        this.dlq = dlq;
    }

    /** The actual (randomized) topic name created on the broker for this test. */
    public String topicName() {
        return topicName;
    }

    /**
     * Kafka bootstrap-servers string for the shared container. Hand this
     * straight to the Flink job under test's own {@code KafkaSource}/
     * {@code KafkaSink} builders, along with {@link #topicName()}, so the
     * job talks to the exact same broker and topic this handle uses.
     */
    public String bootstrapServers() {
        return bootstrapServers;
    }

    /** Declared serialization format for this topic. */
    public Format format() {
        return format;
    }

    /**
     * Schema Registry URL when {@link #format()} is {@link Format#AVRO};
     * {@code null} for JSON topics. Pass this into the job under test's
     * Confluent Avro serde config.
     */
    public String schemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    /** Whether this topic was declared with {@link KafkaTopic#dlq()}{@code true}. */
    public boolean hasDlq() {
        return dlq != null;
    }

    /**
     * Companion DLQ handle when {@link KafkaTopic#dlq()} is {@code true}.
     *
     * @throws IllegalStateException if no DLQ was requested for this topic
     */
    public DlqHandle dlq() {
        if (dlq == null) {
            throw new IllegalStateException(
                "Topic '" + topicName + "' has no DLQ. Declare @KafkaTopic(dlq = true) to enable one.");
        }
        return dlq;
    }

    /**
     * DLQ topic name for configuring the job under test.
     *
     * @throws IllegalStateException if no DLQ was requested for this topic
     */
    public String dlqTopicName() {
        return dlq().topicName();
    }

    /**
     * Blocks until at least {@code expectedCount} raw payloads arrive on the
     * companion DLQ topic.
     *
     * @throws IllegalStateException if no DLQ was requested for this topic
     */
    public List<byte[]> awaitDlqRecords(int expectedCount, Duration timeout) {
        return dlq().awaitRecords(expectedCount, timeout);
    }

    /**
     * Blocks until the DLQ's accumulated {@link ConsumedRecord}s satisfy
     * {@code condition}.
     *
     * @throws IllegalStateException if no DLQ was requested for this topic
     */
    public List<ConsumedRecord<byte[]>> awaitDlqRecords(
            Predicate<List<ConsumedRecord<byte[]>>> condition, Duration timeout) {
        return dlq().awaitConsumedRecords(condition, timeout);
    }

    /**
     * Serializes and sends each record to this topic with no explicit key
     * or headers, blocking until the broker has acknowledged all of them.
     */
    @SafeVarargs
    public final void produce(T... records) {
        produce(null, Collections.emptyMap(), records);
    }

    /**
     * Serializes and sends each record to this topic under the given key,
     * with no headers, blocking until the broker has acknowledged all of them.
     */
    @SafeVarargs
    public final void produce(String key, T... records) {
        produce(key, Collections.emptyMap(), records);
    }

    /**
     * Serializes and sends a single record with an explicit key and headers.
     * Header values are written as UTF-8 bytes.
     */
    public void produce(String key, Map<String, String> headers, T record) {
        produce(key, headers, asArray(record));
    }

    /**
     * Serializes and sends each record under the same key and headers,
     * blocking until the broker has acknowledged all of them.
     */
    @SafeVarargs
    public final void produce(String key, Map<String, String> headers, T... records) {
        if (records == null || records.length == 0) {
            throw new IllegalArgumentException("At least one record is required");
        }
        Map<String, String> safeHeaders = headers == null ? Collections.emptyMap() : headers;
        try {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (T record : records) {
                byte[] payload = codec.encode(topicName, record);
                ProducerRecord<String, byte[]> producerRecord =
                    new ProducerRecord<>(topicName, null, key, payload);
                for (Map.Entry<String, String> header : safeHeaders.entrySet()) {
                    byte[] headerValue = header.getValue() == null
                        ? null
                        : header.getValue().getBytes(StandardCharsets.UTF_8);
                    producerRecord.headers().add(new RecordHeader(header.getKey(), headerValue));
                }
                futures.add(producer().send(producerRecord));
            }
            for (Future<RecordMetadata> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce records to topic '" + topicName + "'", e);
        }
    }

    /**
     * Publishes raw UTF-8 payloads that deliberately bypass typed
     * serialization — use this to inject malformed JSON / garbage bytes so
     * the job under test can prove it routes poison messages to a DLQ.
     */
    public void produceInvalid(String... utf8Payloads) {
        produceInvalid(null, Collections.emptyMap(), utf8Payloads);
    }

    /**
     * Publishes raw UTF-8 poison payloads under the given key and headers.
     */
    public void produceInvalid(String key, Map<String, String> headers, String... utf8Payloads) {
        if (utf8Payloads == null || utf8Payloads.length == 0) {
            throw new IllegalArgumentException("At least one invalid payload is required");
        }
        byte[][] raw = new byte[utf8Payloads.length][];
        for (int i = 0; i < utf8Payloads.length; i++) {
            raw[i] = utf8Payloads[i].getBytes(StandardCharsets.UTF_8);
        }
        produceRaw(key, headers, raw);
    }

    /**
     * Publishes raw byte payloads that bypass typed serialization.
     */
    public void produceRaw(byte[]... payloads) {
        produceRaw(null, Collections.emptyMap(), payloads);
    }

    /**
     * Publishes raw byte payloads under the given key and headers, bypassing
     * typed serialization.
     */
    public void produceRaw(String key, Map<String, String> headers, byte[]... payloads) {
        if (payloads == null || payloads.length == 0) {
            throw new IllegalArgumentException("At least one raw payload is required");
        }
        Map<String, String> safeHeaders = headers == null ? Collections.emptyMap() : headers;
        try {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (byte[] payload : payloads) {
                ProducerRecord<String, byte[]> producerRecord =
                    new ProducerRecord<>(topicName, null, key, payload);
                for (Map.Entry<String, String> header : safeHeaders.entrySet()) {
                    byte[] headerValue = header.getValue() == null
                        ? null
                        : header.getValue().getBytes(StandardCharsets.UTF_8);
                    producerRecord.headers().add(new RecordHeader(header.getKey(), headerValue));
                }
                futures.add(producer().send(producerRecord));
            }
            for (Future<RecordMetadata> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to produce raw/invalid records to topic '" + topicName + "'", e);
        }
    }

    /**
     * Blocks until at least {@code expectedCount} records have been
     * consumed from this topic (read from the beginning), or the timeout
     * elapses.
     *
     * @throws AssertionError if fewer than {@code expectedCount} records
     *         arrive before the timeout
     */
    public List<T> awaitRecords(int expectedCount, Duration timeout) {
        return values(awaitConsumedRecords(expectedCount, timeout));
    }

    /**
     * Blocks until the accumulated list of consumed values satisfies
     * {@code condition}, or the timeout elapses.
     *
     * @throws AssertionError if the condition is not met before the timeout
     */
    public List<T> awaitRecords(Predicate<List<T>> condition, Duration timeout) {
        return values(awaitConsumedRecords(
            records -> condition.test(values(records)),
            timeout));
    }

    /**
     * Like {@link #awaitRecords(int, Duration)}, but returns full
     * {@link ConsumedRecord}s so tests can assert on keys, headers,
     * partition, and offset.
     */
    public List<ConsumedRecord<T>> awaitConsumedRecords(int expectedCount, Duration timeout) {
        return awaitConsumedRecords(records -> records.size() >= expectedCount, timeout);
    }

    /**
     * Blocks until the accumulated {@link ConsumedRecord} list satisfies
     * {@code condition}, or the timeout elapses.
     *
     * @throws AssertionError if the condition is not met before the timeout
     */
    public List<ConsumedRecord<T>> awaitConsumedRecords(
            Predicate<List<ConsumedRecord<T>>> condition, Duration timeout) {
        List<ConsumedRecord<T>> collected = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);

        try (KafkaConsumer<String, byte[]> consumer = newConsumer()) {
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
            "Timed out after " + timeout + " waiting for expected records on topic '" + topicName
                + "'. Collected " + collected.size() + " record(s) so far: " + collected);
    }

    /**
     * Waits until at least {@code expectedTotalCount} records have arrived,
     * then returns them grouped by Kafka key (null keys grouped under
     * {@code null}). Within each key, values appear in consumption order,
     * which for a single partition matches produce order.
     *
     * <p>Use this when the job under test has per-key ordering guarantees
     * that a flat {@link #awaitRecords} list cannot express cleanly.
     */
    public Map<String, List<T>> awaitRecordsByKey(int expectedTotalCount, Duration timeout) {
        return awaitRecordsByKey(
            byKey -> byKey.values().stream().mapToInt(List::size).sum() >= expectedTotalCount,
            timeout);
    }

    /**
     * Blocks until the key-grouped map of values satisfies {@code condition}.
     */
    public Map<String, List<T>> awaitRecordsByKey(
            Predicate<Map<String, List<T>>> condition, Duration timeout) {
        List<ConsumedRecord<T>> consumed = awaitConsumedRecords(
            records -> condition.test(groupValuesByKey(records)),
            timeout);
        return groupValuesByKey(consumed);
    }

    /**
     * Waits until at least {@code expectedTotalCount} records have arrived,
     * then returns them grouped by partition id.
     */
    public Map<Integer, List<T>> awaitRecordsByPartition(int expectedTotalCount, Duration timeout) {
        List<ConsumedRecord<T>> consumed = awaitConsumedRecords(expectedTotalCount, timeout);
        Map<Integer, List<T>> byPartition = new LinkedHashMap<>();
        for (ConsumedRecord<T> record : consumed) {
            byPartition.computeIfAbsent(record.partition(), ignored -> new ArrayList<>())
                .add(record.value());
        }
        return Collections.unmodifiableMap(byPartition);
    }

    private KafkaConsumer<String, byte[]> newConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-testkit-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private ConsumedRecord<T> toConsumedRecord(ConsumerRecord<String, byte[]> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            if (header.value() == null) {
                headers.put(header.key(), null);
            } else {
                headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        return new ConsumedRecord<>(
            record.key(),
            codec.decode(topicName, record.value()),
            headers,
            record.partition(),
            record.offset());
    }

    private Map<String, List<T>> groupValuesByKey(List<ConsumedRecord<T>> records) {
        Map<String, List<T>> byKey = new LinkedHashMap<>();
        for (ConsumedRecord<T> record : records) {
            byKey.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(record.value());
        }
        return Collections.unmodifiableMap(byKey);
    }

    private List<T> values(List<ConsumedRecord<T>> records) {
        List<T> values = new ArrayList<>(records.size());
        for (ConsumedRecord<T> record : records) {
            values.add(record.value());
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private T[] asArray(T record) {
        T[] array = (T[]) java.lang.reflect.Array.newInstance(valueType, 1);
        array[0] = record;
        return array;
    }

    private KafkaProducer<String, byte[]> producer() {
        KafkaProducer<String, byte[]> local = producer;
        if (local == null) {
            synchronized (this) {
                local = producer;
                if (local == null) {
                    Properties props = new Properties();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                    local = new KafkaProducer<>(props);
                    producer = local;
                }
            }
        }
        return local;
    }

    /** Called by the Kafka plugin during teardown; not for test code to call directly. */
    void close() {
        KafkaProducer<String, byte[]> local = producer;
        if (local != null) {
            local.close();
        }
        codec.close();
    }
}
