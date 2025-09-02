package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Serializer for java.time.LocalDateTime that writes milliseconds since Unix epoch (UTC).
 */
public class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {
    @Override
    public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        long epochMillis = value.toInstant(ZoneOffset.UTC).toEpochMilli();
        gen.writeNumber(epochMillis);
    }
}


