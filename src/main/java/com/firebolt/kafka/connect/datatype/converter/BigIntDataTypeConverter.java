package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BigIntDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        // do not check for Double or Float since we do not want to lose precision implicitly. Maybe in the future we can have a flag on the connector configuration for this to be allowed
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

        throw new ColumnConversionFailedException(
                fireboltColumn.getName(), fireboltColumn.getDataType(),
                "Cannot convert kafka message attribute to a bigint due to incompatible type: " + (value != null ? value.getClass().getName() : "null"));
    }

}