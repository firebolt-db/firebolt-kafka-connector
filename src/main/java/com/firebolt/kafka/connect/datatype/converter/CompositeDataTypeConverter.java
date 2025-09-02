package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import org.apache.kafka.connect.data.Schema;

public abstract class CompositeDataTypeConverter extends AbstractColumnTypeConverter {

    protected void convertTimestamp(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            Timestamp timestamp = TimestampUtil.asTimestamp((Long) kafkaMessageColumnValue.getValue());
            statement.setTimestamp(paramIndex, timestamp);
        } else if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            statement.setString(paramIndex, (String) kafkaMessageColumnValue.getValue());
        }

    }
}
