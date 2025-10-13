package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaIntegerDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessIntegerDataTypeConverter extends SchemaIntegerDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        // all ints are converted to long
        if (value instanceof Long) {
            Long longValue = (Long) value;
            if (longValue >= Integer.MIN_VALUE || longValue<= Integer.MAX_VALUE) {
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


