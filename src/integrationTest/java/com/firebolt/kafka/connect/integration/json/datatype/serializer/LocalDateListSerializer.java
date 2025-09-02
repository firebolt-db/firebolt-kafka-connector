package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

/**
 * Serializer for List<LocalDate> that writes each element as days since Unix epoch (1970-01-01).
 */
public class LocalDateListSerializer extends JsonSerializer<List<LocalDate>> {
    @Override
    public void serialize(List<LocalDate> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        gen.writeStartArray();
        for (LocalDate date : value) {
            if (date == null) {
                gen.writeNull();
            } else {
                long epochDay = date.toEpochDay();
                gen.writeNumber(Math.toIntExact(epochDay));
            }
        }
        gen.writeEndArray();
    }
}


