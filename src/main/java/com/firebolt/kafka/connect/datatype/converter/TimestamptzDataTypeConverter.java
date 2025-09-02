package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.apache.kafka.connect.data.Schema;

public class TimestamptzDataTypeConverter extends CompositeDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime((Long) kafkaMessageColumnValue.getValue());
            statement.setObject(paramIndex, offsetDateTime);
            return;
        } else if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String timestamp = (String) kafkaMessageColumnValue.getValue();

            // make sure we can convert it to a valid timestamptz value
            if (FireboltTimestamptzConverter.isValidTimestamptz(timestamp)) {
                statement.setString(paramIndex, timestamp);
                return;
            }
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert value to valid timestamptz column");
    }
}
