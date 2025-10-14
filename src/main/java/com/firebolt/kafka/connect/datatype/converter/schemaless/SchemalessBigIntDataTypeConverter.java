package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.NumericDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessBigIntDataTypeConverter extends NumericDataTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            statement.setLong(paramIndex, ((Number) value).longValue());
            return;
        }

        if (value instanceof String) {
            String stringValue = (String) value;
            try {
                long parsed = Long.parseLong(stringValue.trim());
                statement.setLong(paramIndex, parsed);
                return;
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a bigint due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


