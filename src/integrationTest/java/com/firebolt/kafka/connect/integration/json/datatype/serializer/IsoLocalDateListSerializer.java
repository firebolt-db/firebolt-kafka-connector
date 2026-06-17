package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

/**
 * Serializer for List&lt;LocalDate&gt; that emits each element as an ISO-8601 date string
 * (yyyy-MM-dd). Used for the JSON-Schema (read_avro) path where ARRAY(DATE) columns must be fed via
 * ISO date strings.
 */
public class IsoLocalDateListSerializer extends JsonSerializer<List<LocalDate>> {
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
                gen.writeString(date.toString());
            }
        }
        gen.writeEndArray();
    }
}
