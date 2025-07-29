package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.kafka.connect.data.Schema;

public class TimestampDataTypeConverter implements ColumnDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            Timestamp timestamp = TimestampUtil.asTimestamp((Long) kafkaMessageColumnValue.getValue());
            statement.setTimestamp(paramIndex, timestamp);
        } else if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            statement.setString(paramIndex, (String) kafkaMessageColumnValue.getValue());
        }
    }
}
