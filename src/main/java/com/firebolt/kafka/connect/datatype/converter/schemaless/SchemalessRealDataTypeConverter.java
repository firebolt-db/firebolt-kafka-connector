package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.NumericDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessRealDataTypeConverter extends NumericDataTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Float) {
            // NOTE: the Float.MAX_VALUE will not be set in firebolt if we are using setFloat method, as it fails with the following error
            // Value of type double precision cannot be safely converted into type real. Need to do more investigation in phase 2
            statement.setString(paramIndex, String.valueOf(value));
            return;
        }

        // integers are deserialized as longs
        if (value instanceof Long) {
            Long longValue = (Long) value;
            if (longValue >= Integer.MIN_VALUE || longValue <= Integer.MAX_VALUE) {
                float f = (longValue).floatValue();
                statement.setFloat(paramIndex, f);
                return;
            }
        }

        // floating numbers are deserialized as Double
        if (value instanceof Double) {
            Double doubleValue = (Double) value;

            // only proceed if the value is in between the float ranges
            if (doubleValue >= -Float.MAX_VALUE || doubleValue <= Float.MAX_VALUE) {
                // NOTE: the Float.MAX_VALUE will not be set in firebolt if we are using setFloat method, as it fails with the following error
                // Value of type double precision cannot be safely converted into type real. Need to do more investigation in phase 2
                statement.setString(paramIndex, String.valueOf(doubleValue));
                return;
            }
        }

        if (value instanceof String) {
            String s = (String) value;
            try {
                float f = Float.parseFloat(s.trim());
                statement.setString(paramIndex, String.valueOf(f));
                return;
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a real due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


