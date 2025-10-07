package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.IntegerDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessIntegerDataTypeConverter extends IntegerDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value instanceof Number) {
            int intValue = ((Number) value).intValue();
            statement.setInt(paramIndex, intValue);
            return;
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


