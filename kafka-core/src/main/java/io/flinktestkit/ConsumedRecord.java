package io.flinktestkit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A single Kafka record as observed by {@link TopicHandle} while awaiting
 * output — value plus the metadata tests need for Phase 2 assertions
 * (key, headers, partition, offset).
 *
 * @param <T> the deserialized value type
 */
public final class ConsumedRecord<T> {

    private final String key;
    private final T value;
    private final Map<String, String> headers;
    private final int partition;
    private final long offset;

    ConsumedRecord(String key, T value, Map<String, String> headers, int partition, long offset) {
        this.key = key;
        this.value = Objects.requireNonNull(value, "value");
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.partition = partition;
        this.offset = offset;
    }

    /** Record key, or {@code null} if the producer did not set one. */
    public String key() {
        return key;
    }

    public T value() {
        return value;
    }

    /** String headers (UTF-8). Missing header keys are absent from the map. */
    public Map<String, String> headers() {
        return headers;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "ConsumedRecord{key=" + key
            + ", value=" + value
            + ", headers=" + headers
            + ", partition=" + partition
            + ", offset=" + offset
            + '}';
    }
}
