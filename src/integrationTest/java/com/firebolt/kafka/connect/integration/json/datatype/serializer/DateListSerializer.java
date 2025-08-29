package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.ZoneId;
import java.util.List;

/**
 * Custom serializer for List<java.util.Date> to encode each element as days since Unix epoch (1970-01-01).
 * Uses the system default time zone to match DateSerializer behavior.
 */
public class DateListSerializer extends JsonSerializer<List<java.util.Date>> {
    @Override
    public void serialize(List<java.util.Date> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }

        ZoneId defaultZoneId = ZoneId.systemDefault();
        gen.writeStartArray();
        for (java.util.Date date : value) {
            if (date == null) {
                gen.writeNull();
            } else {
                long epochDay = date.toInstant().atZone(defaultZoneId).toLocalDate().toEpochDay();
                gen.writeNumber(Math.toIntExact(epochDay));
            }
        }
        gen.writeEndArray();
    }
}


