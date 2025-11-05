package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.NumericDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessIntegerDataTypeConverter extends NumericDataTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            statement.setInt(paramIndex, ((Number) value).intValue());
            return;
        }

        if (value instanceof Long) {
            Long longValue = (Long) value;
            if (longValue >= Integer.MIN_VALUE && longValue<= Integer.MAX_VALUE) {
                // then we can convert to int
                statement.setInt(paramIndex, longValue.intValue());
                return;
            }
        }

        if (value instanceof String) {
            String str = (String) value;
            try {
                int parsed = Integer.parseInt(str.trim());
                statement.setInt(paramIndex, parsed);
                return;
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert kafka message attribute to a integer due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


