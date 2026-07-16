package io.flinktestkit.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.flinktestkit.ValueCodec;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Confluent-wire-format Avro codec backed by Schema Registry.
 * Schemas auto-register on first encode.
 */
final class AvroValueCodec<T extends SpecificRecord> implements ValueCodec<T> {

    private final Class<T> valueType;
    private final KafkaAvroSerializer serializer;
    private final KafkaAvroDeserializer deserializer;

    AvroValueCodec(Class<T> valueType, String schemaRegistryUrl) {
        if (!SpecificRecord.class.isAssignableFrom(valueType)) {
            throw new IllegalArgumentException(
                "Format.AVRO requires valueType to implement org.apache.avro.specific.SpecificRecord, got "
                    + valueType.getName());
        }
        this.valueType = valueType;

        Map<String, Object> serializerConfig = new HashMap<>();
        serializerConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serializerConfig.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);

        this.serializer = new KafkaAvroSerializer();
        this.serializer.configure(serializerConfig, false);

        Map<String, Object> deserializerConfig = new HashMap<>(serializerConfig);
        deserializerConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        this.deserializer = new KafkaAvroDeserializer();
        this.deserializer.configure(deserializerConfig, false);
    }

    @Override
    public byte[] encode(String topic, T value) {
        return serializer.serialize(topic, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T decode(String topic, byte[] bytes) {
        Object decoded = deserializer.deserialize(topic, bytes);
        if (decoded == null) {
            return null;
        }
        if (!valueType.isInstance(decoded)) {
            throw new IllegalStateException(
                "Deserialized Avro value type " + decoded.getClass().getName()
                    + " is not assignable to " + valueType.getName());
        }
        return (T) decoded;
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }
}
