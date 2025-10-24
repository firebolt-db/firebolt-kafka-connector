package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemalessTextDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();
        if (value instanceof Map) {
            // if the instance is a map then it can be that this is a json and we should use a serializer to save it
            try {
                String serializedValue = OBJECT_MAPPER.writeValueAsString(value);
                statement.setString(paramIndex, serializedValue);
                return;
            } catch (JsonProcessingException e) {
                log.error("Failed to serialized the message as json in column {}", fireboltColumn.getName());
                aColumnConversionFailedException(fireboltColumn, value);
            }
        }

        statement.setString(paramIndex, String.valueOf(schemalessKafkaMessageColumnValue.getValue()));
    }
}


