package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.NumericDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.kafka.connect.data.Schema;

public class SchemaDecimalDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String stringValue = (String) kafkaMessageColumnValue.getValue();
            try {
                new BigDecimal(stringValue.trim());
            } catch (Exception ex) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a decimal value in firebolt");
            }
            statement.setString(paramIndex, stringValue);
            return;
        }

        if (value instanceof BigDecimal) {
            statement.setString(paramIndex, value.toString());
            return;
        }

        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            long longValue = ((Number) value).longValue();
            statement.setString(paramIndex, BigDecimal.valueOf(longValue).toString());
            return;
        }

        if (value instanceof Float || value instanceof Double) {
            double doubleValue = ((Number) value).doubleValue();
            statement.setString(paramIndex, BigDecimal.valueOf(doubleValue).toString());
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


