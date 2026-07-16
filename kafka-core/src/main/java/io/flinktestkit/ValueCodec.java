package io.flinktestkit;

/**
 * Encodes and decodes topic values for a single {@link TopicHandle}.
 *
 * @param <T> value type
 */
public interface ValueCodec<T> {

    byte[] encode(String topic, T value);

    T decode(String topic, byte[] bytes);

    default void close() {
    }
}
