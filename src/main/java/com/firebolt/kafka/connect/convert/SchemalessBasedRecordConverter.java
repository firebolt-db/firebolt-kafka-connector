package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Converter for schemaless records produced by JsonConverter with schemas.enable=false.
 * Expects the record value to be a Map<String, Object> representing the JSON payload.
 */
@Slf4j
public class SchemalessBasedRecordConverter extends RecordConverter {

    public SchemalessBasedRecordConverter(SinkConfig config) {
        super(config);
    }

    @Override
    public boolean canHandle(SinkRecord record) {
        return record.valueSchema() == null && record.value() instanceof Map;
    }

    @Override
    public String getDescription() {
        return "Handles schemaless JSON records (Map-based values from JsonConverter without schemas)";
    }

    @Override
    protected Map<String, KafkaMessageColumnValue> convertRecordValue(SinkRecord record) throws RecordConversionException {
        Object value = record.value();
        if (value == null) {
            return handleNullValue(record);
        }

        if (!(value instanceof Map)) {
            throw new RecordConversionException(
                    String.format("Expected Map value for schemaless record, but got %s",
                            value.getClass().getSimpleName()));
        }

        Map<?, ?> source = (Map<?, ?>) value;
        Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();

        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object rawKey = entry.getKey();
            if (!(rawKey instanceof String)) {
                // Skip non-string keys; schemaless JSON objects should use string keys
                continue;
            }
            String fieldName = (String) rawKey;
            Object fieldValue = entry.getValue();

            KafkaMessageColumnValue.KafkaMessageColumnValueBuilder builder = KafkaMessageColumnValue.builder()
                    .value(fieldValue)
                    .schemaType(inferSchemaType(fieldValue));

            if (fieldValue instanceof List) {
                builder.schemaSubType(inferArrayElementSchemaType((List<?>) fieldValue));
            }

            columnValues.put(fieldName, builder.build());
        }

        return columnValues;
    }

    private Schema.Type inferSchemaType(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return Schema.Type.STRING;
        }
        if (value instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        }
        if (value instanceof Integer) {
            return Schema.Type.INT32;
        }
        if (value instanceof Long) {
            return Schema.Type.INT64;
        }
        if (value instanceof Float) {
            return Schema.Type.FLOAT32;
        }
        if (value instanceof Double) {
            return Schema.Type.FLOAT64;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return Schema.Type.BYTES;
        }
        if (value instanceof List) {
            return Schema.Type.ARRAY;
        }
        if (value instanceof Map) {
            return Schema.Type.MAP;
        }
        // Fallback: represent as string
        return Schema.Type.STRING;
    }

    private Schema.Type inferArrayElementSchemaType(List<?> elements) {
        for (Object e : elements) {
            if (e == null) {
                continue;
            }
            return inferSchemaType(e);
        }
        return null;
    }
}


