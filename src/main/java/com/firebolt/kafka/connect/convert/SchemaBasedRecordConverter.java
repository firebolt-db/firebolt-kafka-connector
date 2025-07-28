package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Converter for SinkRecords that have an embedded schema and are typically Struct values.
 * This handles records coming from converters like Avro or JSON with schema registry.
 */
@Slf4j
public class SchemaBasedRecordConverter extends RecordConverter {

    /**
     * Constructor for schema-based record converter.
     *
     * @param config the sink configuration
     */
    public SchemaBasedRecordConverter(SinkConfig config) {
        super(config);
    }

    @Override
    public boolean canHandle(SinkRecord record) {
        return record.valueSchema() != null && record.value() instanceof Struct;
    }

    @Override
    public String getDescription() {
        return "Handles records with embedded schemas (typically Struct values from Avro/JSON+Schema)";
    }

    @Override
    protected Map<String, KafkaMessageColumnValue> convertRecordValue(SinkRecord record) throws RecordConversionException {
        Object value = record.value();
        Schema valueSchema = record.valueSchema();

        if (value == null) {
            return handleNullValue(record);
        }

        if (!(value instanceof Struct)) {
            throw new RecordConversionException(
                    String.format("Expected Struct value with schema, but got %s",
                            value.getClass().getSimpleName()));
        }

        return convertStruct((Struct) value, valueSchema);
    }

    /**
     * Converts a Struct with its schema to a map of column values.
     *
     * @param struct the Struct value
     * @param schema the corresponding schema
     * @return map of column names to converted values
     */
    private Map<String, KafkaMessageColumnValue> convertStruct(Struct struct, Schema schema) {
        Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Object fieldValue = struct.get(fieldName);

            KafkaMessageColumnValue convertedValue = convertValue(fieldValue, field.schema());
            columnValues.put(fieldName, convertedValue);
        }

        return columnValues;
    }

    /**
     * Converts a field value based on its schema type.
     *
     * @param value the field value
     * @param schema the field schema
     * @return the converted value suitable for Firebolt
     */
    private KafkaMessageColumnValue convertValue(Object value, Schema schema) {
        if (value == null) {
            return null;
        }

        return KafkaMessageColumnValue.builder()
                .value(value)
                .schemaType(schema.type())
                .schemaTypeParams(schema.parameters())
                .build();
    }

} 