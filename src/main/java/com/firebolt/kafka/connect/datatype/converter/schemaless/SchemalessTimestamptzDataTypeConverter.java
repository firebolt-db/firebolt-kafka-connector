package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

public class SchemalessTimestamptzDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();
        if (value instanceof String) {
            String timestamp = (String) schemalessKafkaMessageColumnValue.getValue();

            // make sure we can convert it to a valid timestamptz value
            if (FireboltTimestamptzConverter.isValidTimestamptz(timestamp)) {
                statement.setString(paramIndex, timestamp);
                return;
            }
        }

        if (value instanceof Number) {
            long millisFromEpoch = ((Number) schemalessKafkaMessageColumnValue.getValue()).longValue();
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime(millisFromEpoch);
            statement.setObject(paramIndex, offsetDateTime);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, schemalessKafkaMessageColumnValue.getValue());
    }
}


