package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

public class SchemaTimestampDataTypeConverter extends AbstractColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemaKafkaMessageColumnValue.getValue();
        if (value instanceof Date) {
            Date date = (Date) schemaKafkaMessageColumnValue.getValue();
            statement.setTimestamp(paramIndex, new Timestamp(date.getTime()));
            return;
        }

        if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String dateTimeAsString = (String) schemaKafkaMessageColumnValue.getValue();
            if (FireboltTimestampConverter.isIsoLocalDateTime(dateTimeAsString)) {
                statement.setString(paramIndex, dateTimeAsString);
                return;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "String value cannot be converted to a timestamp column in firebolt as it is not an ISO local ");
        }

        if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            Timestamp timestamp = TimestampUtil.asTimestamp((Long) schemaKafkaMessageColumnValue.getValue());
            statement.setTimestamp(paramIndex, timestamp);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, value);

    }
}


