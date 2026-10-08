package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Custom serializer for List<OffsetDateTime> to convert each element to timestamp values.
 */
public class OffsetDateTimeListSerializer extends JsonSerializer<List<OffsetDateTime>> {
    @Override
    public void serialize(List<OffsetDateTime> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }

        gen.writeStartArray();
        for (OffsetDateTime dateTime : value) {
            if (dateTime == null) {
                gen.writeNull();
            } else {
                // Convert to milliseconds since epoch (Kafka Connect Timestamp logical type)
                gen.writeNumber(dateTime.toInstant().toEpochMilli());
            }
        }
        gen.writeEndArray();
    }
}
