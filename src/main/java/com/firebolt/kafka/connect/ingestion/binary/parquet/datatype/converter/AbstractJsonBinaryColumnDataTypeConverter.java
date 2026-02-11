package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.util.Collection;
import java.util.Map;

/**
 * Converts values to JSON strings for binary parquet ingestion into Firebolt JSON columns.
 * 
 * Handles all valid JSON value types:
 * - Maps and Collections are serialized to JSON
 * - Strings are used as-is (assumed to be valid JSON literals or JSON strings)
 * - Numbers and Booleans are converted to their JSON representation
 * - null values are converted to the JSON literal "null"
 */
public abstract class AbstractJsonBinaryColumnDataTypeConverter<T extends SchemalessKafkaMessageColumnValue> extends AbstractBinaryColumnTypeConverter<T, String> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public String toParquetValue(T kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value == null) {
            return null;
        }

        if (value instanceof Map || value instanceof Collection) {
            try {
                return OBJECT_MAPPER.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(),
                        fireboltColumn.getDataType(),
                        "Failed to serialize value to JSON: " + e.getMessage());
            }
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ColumnConversionFailedException(
                    fireboltColumn.getName(),
                    fireboltColumn.getDataType(),
                    "Failed to serialize value to JSON: " + e.getMessage());
        }
    }

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }
}
