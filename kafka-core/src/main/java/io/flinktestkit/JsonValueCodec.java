package io.flinktestkit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

final class JsonValueCodec<T> implements ValueCodec<T> {

    private final Class<T> valueType;
    private final ObjectMapper objectMapper;

    JsonValueCodec(Class<T> valueType) {
        this.valueType = valueType;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] encode(String topic, T value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to JSON-serialize " + valueType.getSimpleName(), e);
        }
    }

    @Override
    public T decode(String topic, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, valueType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to JSON-deserialize " + valueType.getSimpleName(), e);
        }
    }
}
