package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.ZoneId;

/**
 * Custom serializer for java.util.Date to encode as days since Unix epoch (1970-01-01) in UTC.
 * This aligns with Kafka Connect's Date logical type (int32 days).
 */
public class DateSerializer extends JsonSerializer<java.util.Date> {
    @Override
    public void serialize(java.util.Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }

        ZoneId defaultZoneId = ZoneId.systemDefault();
        long epochDay = value.toInstant().atZone(defaultZoneId).toLocalDate().toEpochDay();
        gen.writeNumber(Math.toIntExact(epochDay));
    }
}
