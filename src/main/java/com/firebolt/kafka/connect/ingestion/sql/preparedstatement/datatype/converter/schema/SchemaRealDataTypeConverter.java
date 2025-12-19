package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.NumericDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemaRealDataTypeConverter extends NumericDataTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemaKafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            float f = ((Number) value).floatValue();
            statement.setFloat(paramIndex, f);
            return;
        }

        if (value instanceof Float) {
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
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a real due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


