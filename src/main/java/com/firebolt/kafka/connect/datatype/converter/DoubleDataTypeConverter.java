package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DoubleDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value instanceof Number) {
            double doubleValue = ((Number) value).doubleValue();
            statement.setDouble(paramIndex, doubleValue);
            return;
        }

        if (value instanceof String) {
            String str = ((String) value).trim();
            try {
                double parsed = Double.parseDouble(str);
                statement.setDouble(paramIndex, parsed);
                return;
            } catch (NumberFormatException e) {
                // fall through
            }
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert kafka message attribute to a double value in firebolt");
    }
}
