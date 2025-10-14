package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.NumericDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessDoubleDataTypeConverter extends NumericDataTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

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
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a double due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


