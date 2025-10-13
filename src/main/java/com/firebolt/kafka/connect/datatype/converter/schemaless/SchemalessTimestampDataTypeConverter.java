package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTimestampDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SchemalessTimestampDataTypeConverter extends SchemaTimestampDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();
        if (value instanceof String) {
            String dateTimeAsString = (String) kafkaMessageColumnValue.getValue();
            if (FireboltTimestampConverter.isIsoLocalDateTime(dateTimeAsString)) {
                statement.setString(paramIndex, dateTimeAsString);
                return;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "String value cannot be converted to a timestamp column in firebolt as it is not an ISO local ");
        }

        if (value instanceof Number) {
            long millisFromEpoch = ((Number) kafkaMessageColumnValue.getValue()).longValue();
            Timestamp timestamp = TimestampUtil.asTimestamp(millisFromEpoch);
            statement.setTimestamp(paramIndex, timestamp);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, value);

    }
}


