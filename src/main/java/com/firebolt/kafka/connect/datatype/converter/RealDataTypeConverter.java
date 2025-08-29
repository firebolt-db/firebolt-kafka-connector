package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RealDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            float f = ((Number) value).floatValue();
            statement.setFloat(paramIndex, f);
            return;
        }

        if (value instanceof Float) {
            // NOTE: the Float.MAX_VALUE will not be set in firebolt if we are using setFloat method, as it fails with the following error
            // Value of type double precision cannot be safely converted into type real. Need to do more investigation in phase 2
            statement.setString(paramIndex, String.valueOf(value));
            return;
        }

        if (value instanceof String) {
            String s = (String) value;
            try {
                float f = Float.parseFloat(s.trim());
                statement.setString(paramIndex, String.valueOf(f));
                return;
            } catch (NumberFormatException e) {
                // fall through
            }
        }

        throw new ColumnConversionFailedException(
                fireboltColumn.getName(), fireboltColumn.getDataType(),
                "Cannot convert kafka message attribute to a real value in firebolt");
    }

}
