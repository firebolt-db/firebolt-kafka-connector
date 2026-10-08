package com.firebolt.kafka.connect.integration.json.datatype.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.time.OffsetDateTime;

/**
 * Custom serializer for OffsetDateTime to convert to timestamp value (microseconds since epoch).
 * This ensures compatibility with Kafka Connect Timestamp logical type.
 */
public class OffsetDateTimeSerializer extends JsonSerializer<OffsetDateTime> {
    @Override
    public void serialize(OffsetDateTime value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
        } else {
            // Convert to milliseconds since epoch (Kafka Connect Timestamp logical type)
            long timestampMillis = value.toInstant().toEpochMilli();
            gen.writeNumber(timestampMillis);
        }
    }
}
