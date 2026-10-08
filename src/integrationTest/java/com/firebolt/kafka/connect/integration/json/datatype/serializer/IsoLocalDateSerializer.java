package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.LocalDate;

/**
 * Serializer for java.time.LocalDate that emits an ISO-8601 date string (yyyy-MM-dd).
 * Used for the JSON-Schema (read_avro) path where DATE columns must be fed via ISO date strings.
 */
public class IsoLocalDateSerializer extends JsonSerializer<LocalDate> {
    @Override
    public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        gen.writeString(value.toString());
    }
}
