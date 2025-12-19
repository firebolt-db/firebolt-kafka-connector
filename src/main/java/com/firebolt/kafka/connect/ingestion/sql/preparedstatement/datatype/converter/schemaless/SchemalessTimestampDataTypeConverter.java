package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SchemalessTimestampDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();
        if (value instanceof String) {
            String dateTimeAsString = (String) schemalessKafkaMessageColumnValue.getValue();
            if (FireboltTimestampConverter.isIsoLocalDateTime(dateTimeAsString)) {
                statement.setString(paramIndex, dateTimeAsString);
                return;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "String value cannot be converted to a timestamp column in firebolt as it is not an ISO local ");
        }

        if (value instanceof Number) {
            long millisFromEpoch = ((Number) schemalessKafkaMessageColumnValue.getValue()).longValue();
            Timestamp timestamp = TimestampUtil.asTimestamp(millisFromEpoch);
            statement.setTimestamp(paramIndex, timestamp);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, value);

    }
}


