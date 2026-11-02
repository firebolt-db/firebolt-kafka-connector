package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Custom serializer for java.util.Date that emits an ISO-8601 date string (yyyy-MM-dd).
 *
 * <p>This is used for the JSON-Schema (read_avro) path where DATE columns must be fed via
 * ISO-8601 date strings. JsonSchemaConverter ignores {@code "connect.type": "int32"} and defaults
 * JSON integers to INT64, so a Connect Date logical type (which requires an INT32 base) cannot be
 * built from JSON-Schema. Feeding the value as a date string (text-&gt;date) round-trips correctly.
 */
public class IsoDateSerializer extends JsonSerializer<java.util.Date> {
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_LOCAL_DATE;

    @Override
    public void serialize(java.util.Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        ZoneId defaultZoneId = ZoneId.systemDefault();
        String iso = ISO.format(value.toInstant().atZone(defaultZoneId).toLocalDate());
        gen.writeString(iso);
    }
}
