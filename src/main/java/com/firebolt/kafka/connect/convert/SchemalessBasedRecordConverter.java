package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
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
    protected Map<String, ? extends KafkaMessageColumnValue> convertRecordValue(SinkRecord record) throws RecordConversionException {
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
        Map<String, SchemalessKafkaMessageColumnValue> columnValues = new HashMap<>();

        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object rawKey = entry.getKey();
            if (!(rawKey instanceof String)) {
                // Skip non-string keys; schemaless JSON objects should use string keys
                continue;
            }
            String fieldName = (String) rawKey;
            Object fieldValue = entry.getValue();

            SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue = new SchemalessKafkaMessageColumnValue(fieldValue);

            columnValues.put(fieldName, schemalessKafkaMessageColumnValue);
        }

        return columnValues;
    }

}


