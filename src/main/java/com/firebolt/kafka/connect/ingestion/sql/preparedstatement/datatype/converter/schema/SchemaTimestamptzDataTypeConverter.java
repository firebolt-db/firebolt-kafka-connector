package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.TimestampUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

public class SchemaTimestamptzDataTypeConverter extends AbstractColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemaKafkaMessageColumnValue.getValue();
        // ProtobufConverter maps google.protobuf.Timestamp to Connect Timestamp logical type,
        // whose Java value is java.util.Date (millis since epoch), not Long.
        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            statement.setObject(paramIndex, instant.atOffset(ZoneOffset.UTC));
            return;
        }

        if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.INT64) {
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime((Long) value);
            statement.setObject(paramIndex, offsetDateTime);
            return;
        } else if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String timestamp = (String) value;

            if (FireboltTimestamptzConverter.isValidTimestamptz(timestamp)) {
                statement.setString(paramIndex, timestamp);
                return;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


