package io.flinktestkit;

final class ValueCodecs {

    private ValueCodecs() {
    }

    static <T> ValueCodec<T> create(Format format, Class<T> valueType, String schemaRegistryUrl) {
        return switch (format) {
            case JSON -> new JsonValueCodec<>(valueType);
            case AVRO -> {
                if (schemaRegistryUrl == null || schemaRegistryUrl.isBlank()) {
                    throw new IllegalStateException(
                        "Format.AVRO requires a Schema Registry URL, but none was provided");
                }
                yield AvroFormatPlugins.require().createCodec(valueType, schemaRegistryUrl);
            }
        };
    }
}
