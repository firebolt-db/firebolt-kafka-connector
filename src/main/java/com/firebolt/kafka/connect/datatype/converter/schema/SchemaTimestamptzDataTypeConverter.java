package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.apache.kafka.connect.data.Schema;

public class SchemaTimestamptzDataTypeConverter extends AbstractColumnTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime((Long) kafkaMessageColumnValue.getValue());
            statement.setObject(paramIndex, offsetDateTime);
            return;
        } else if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String timestamp = (String) kafkaMessageColumnValue.getValue();

            if (FireboltTimestamptzConverter.isValidTimestamptz(timestamp)) {
                statement.setString(paramIndex, timestamp);
                return;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, kafkaMessageColumnValue.getValue());
    }
}


