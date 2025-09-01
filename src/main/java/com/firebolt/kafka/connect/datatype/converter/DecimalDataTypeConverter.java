package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.kafka.connect.data.Schema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;

public class DecimalDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        // If schema explicitly says STRING, validate and set as string first
        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String stringValue = (String) kafkaMessageColumnValue.getValue();
            try {
                // Validate string can be parsed as BigDecimal (supports scientific notation)
                new BigDecimal(stringValue.trim());
            } catch (Exception ex) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a decimal value in firebolt");
            }
            statement.setString(paramIndex, stringValue);
            return;
        }

        if (value instanceof BigDecimal) {
            // when FIR-48811 is solved in JDBC and we use a version with the fix in Sink Connect, then we can use setBigDecimal instead of setString
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

        // If no matching type case, throw conversion failure
        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                "Cannot convert kafka message attribute to a decimal value in firebolt");
    }
}
