package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaDateDataTypeConverter extends AbstractColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemaKafkaMessageColumnValue.getValue();
        if (value instanceof Date) {
            Date date = (Date) schemaKafkaMessageColumnValue.getValue();
            statement.setDate(paramIndex, new java.sql.Date(date.getTime()));
            return;
        }

        if (value instanceof String) {
            if (isIsoLocalDate((String) value)) {
                statement.setString(paramIndex, (String) value);
                return;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


