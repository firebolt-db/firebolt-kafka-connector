package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;

public class ByteaDataTypeConverter implements ColumnDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();
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

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert value to bytea column in firebolt");
    }
}
