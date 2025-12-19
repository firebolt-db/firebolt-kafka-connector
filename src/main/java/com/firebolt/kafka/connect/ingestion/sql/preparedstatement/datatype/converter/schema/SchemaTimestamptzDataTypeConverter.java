package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.TimestampUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.apache.kafka.connect.data.Schema;

public class SchemaTimestamptzDataTypeConverter extends AbstractColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime((Long) schemaKafkaMessageColumnValue.getValue());
            statement.setObject(paramIndex, offsetDateTime);
            return;
        } else if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String timestamp = (String) schemaKafkaMessageColumnValue.getValue();

            if (FireboltTimestamptzConverter.isValidTimestamptz(timestamp)) {
                statement.setString(paramIndex, timestamp);
                return;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, schemaKafkaMessageColumnValue.getValue());
    }
}


