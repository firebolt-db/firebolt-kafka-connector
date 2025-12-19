package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.FireboltByteaConverter;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemaByteaDataTypeConverter extends AbstractColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemaKafkaMessageColumnValue.getValue();
        if (value instanceof byte[]) {
            statement.setBytes(paramIndex, FireboltByteaConverter.convertFireboltBytea((byte[]) value));
            return;
        }

        if (value instanceof ByteBuffer) {
            statement.setBytes(paramIndex, FireboltByteaConverter.convertFireboltBytea(((ByteBuffer) value).array()));
            return;
        }

        if (value instanceof String) {
            statement.setString(paramIndex, (String) value);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


