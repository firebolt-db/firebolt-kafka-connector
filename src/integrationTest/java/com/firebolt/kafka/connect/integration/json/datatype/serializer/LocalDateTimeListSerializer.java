package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Serializer for List<LocalDateTime> that writes an array of epoch milliseconds (UTC), preserving nulls.
 */
public class LocalDateTimeListSerializer extends JsonSerializer<List<LocalDateTime>> {
    @Override
    public void serialize(List<LocalDateTime> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        gen.writeStartArray();
        for (LocalDateTime ldt : value) {
            if (ldt == null) {
                gen.writeNull();
            } else {
                gen.writeNumber(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
            }
        }
        gen.writeEndArray();
    }
}


