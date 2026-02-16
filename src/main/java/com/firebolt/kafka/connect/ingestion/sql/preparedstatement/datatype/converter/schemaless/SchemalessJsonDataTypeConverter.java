package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.AbstractColumnTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts schemaless Kafka message values to JSON strings for SQL ingestion into Firebolt JSON columns.
 * 
 * Handles all valid JSON value types:
 * - Maps and Collections are serialized to JSON
 * - Strings are used as-is (assumed to be valid JSON literals or JSON strings)
 * - Numbers and Booleans are converted to their JSON representation
 * - null values are converted to the JSON literal "null"
 */
@Slf4j
public class SchemalessJsonDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value == null) {
            statement.setString(paramIndex, null);
            return;
        }

        if (value instanceof Map || value instanceof Collection) {
            try {
                String serializedValue = OBJECT_MAPPER.writeValueAsString(value);
                statement.setString(paramIndex, serializedValue);
                return;
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize value to JSON for column {}: {}", fireboltColumn.getName(), e.getMessage());
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(),
                        fireboltColumn.getDataType(),
                        "Failed to serialize value to JSON: " + e.getMessage());
            }
        }

        if (value instanceof String) {
            statement.setString(paramIndex, (String) value);
            return;
        }

        if (value instanceof Number || value instanceof Boolean) {
            statement.setString(paramIndex, String.valueOf(value));
            return;
        }

        // For any other type, try to serialize it as JSON
        try {
            String serializedValue = OBJECT_MAPPER.writeValueAsString(value);
            statement.setString(paramIndex, serializedValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize value to JSON for column {}: {}", fireboltColumn.getName(), e.getMessage());
            throw new ColumnConversionFailedException(
                    fireboltColumn.getName(),
                    fireboltColumn.getDataType(),
                    "Failed to serialize value to JSON: " + e.getMessage());
        }
    }
}
