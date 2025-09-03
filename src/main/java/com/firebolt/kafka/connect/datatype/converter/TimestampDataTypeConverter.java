package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

public class TimestampDataTypeConverter extends AbstractColumnTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();
        if (value instanceof Date) {
            Date date = (Date) kafkaMessageColumnValue.getValue();
            statement.setTimestamp(paramIndex, new Timestamp(date.getTime()));
            return;
        }

        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String dateTimeAsString = (String) kafkaMessageColumnValue.getValue();
            if (FireboltTimestampConverter.isIsoLocalDateTime(dateTimeAsString)) {
                statement.setString(paramIndex, dateTimeAsString);
                return;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "String value cannot be converted to a timestamp column in firebolt");
        }

        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            Timestamp timestamp = TimestampUtil.asTimestamp((Long) kafkaMessageColumnValue.getValue());
            statement.setTimestamp(paramIndex, timestamp);
            return;
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert to valid timestamp in firebolt");

    }
}
