package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTimestamptzDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

public class SchemalessTimestamptzDataTypeConverter extends SchemaTimestamptzDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();
        if (value instanceof String) {
            String timestamp = (String) kafkaMessageColumnValue.getValue();

            // make sure we can convert it to a valid timestamptz value
            if (FireboltTimestamptzConverter.isValidTimestamptz(timestamp)) {
                statement.setString(paramIndex, timestamp);
                return;
            }
        }

        if (value instanceof Number) {
            long millisFromEpoch = ((Number) kafkaMessageColumnValue.getValue()).longValue();
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime(millisFromEpoch);
            statement.setObject(paramIndex, offsetDateTime);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, kafkaMessageColumnValue.getValue());
    }
}


