package io.flinktestkit.example;

/**
 * Shared JSON helpers for Phase 2 example Flink jobs.
 */
final class ExampleJson {

    static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    private ExampleJson() {
    }

    static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize " + type.getSimpleName(), e);
        }
    }

    static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize " + value.getClass().getSimpleName(), e);
        }
    }
}
