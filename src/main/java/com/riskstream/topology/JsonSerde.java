package com.riskstream.topology;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/** JSON serialisation of a record type for Kafka topics and state stores. */
public final class JsonSerde<T> implements Serde<T> {

    /** Unknown fields are tolerated so a producer can add a field without breaking this consumer. */
    static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    public static <T> JsonSerde<T> of(Class<T> type) {
        return new JsonSerde<>(type);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> {
            try {
                return value == null ? null : MAPPER.writeValueAsBytes(value);
            } catch (JacksonException e) {
                throw new SerializationException("Cannot write " + type.getSimpleName() + " as JSON", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            try {
                return bytes == null ? null : MAPPER.readValue(bytes, type);
            } catch (JacksonException e) {
                throw new SerializationException("Cannot read " + type.getSimpleName() + " from topic " + topic, e);
            }
        };
    }
}
