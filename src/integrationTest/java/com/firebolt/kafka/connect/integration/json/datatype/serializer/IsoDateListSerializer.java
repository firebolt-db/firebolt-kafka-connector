package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Custom serializer for List&lt;java.util.Date&gt; that emits each element as an ISO-8601 date
 * string (yyyy-MM-dd). Used for the JSON-Schema (read_avro) path where ARRAY(DATE) columns must be
 * fed via ISO-8601 date strings rather than a Connect Date logical type.
 */
public class IsoDateListSerializer extends JsonSerializer<List<java.util.Date>> {
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_LOCAL_DATE;

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
                gen.writeString(ISO.format(date.toInstant().atZone(defaultZoneId).toLocalDate()));
            }
        }
        gen.writeEndArray();
    }
}
