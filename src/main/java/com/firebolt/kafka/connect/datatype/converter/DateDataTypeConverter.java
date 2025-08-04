package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import org.apache.kafka.connect.data.Schema;

public class DateDataTypeConverter implements ColumnDataTypeConverter{

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.INT32) {
            // The Kafka Connect Date logical type represents dates as the number of days since Unix epoch (1970-01-01) using an int32. So LocalDate.of(2024, 1, 15) is being correctly serialized as 19737 (days since epoch)
            statement.setDate(paramIndex, TimestampUtil.fromDaysSinceEpoch((Integer) kafkaMessageColumnValue.getValue()));
        } else if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            statement.setString(paramIndex, (String) kafkaMessageColumnValue.getValue());
        }
    }
}
