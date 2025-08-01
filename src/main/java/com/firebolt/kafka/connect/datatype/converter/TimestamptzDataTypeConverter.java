package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
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
        } else if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            statement.setString(paramIndex, (String) kafkaMessageColumnValue.getValue());
        }
    }
}
