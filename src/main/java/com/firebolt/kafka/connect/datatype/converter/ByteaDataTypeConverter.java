package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;

public class ByteaDataTypeConverter implements ColumnDataTypeConverter {
    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        String base64Encoded = (String) kafkaMessageColumnValue.getValue();
        if (base64Encoded.equals("")) {
            // In firebolt and empty byte is represented by \x
            statement.setBytes(paramIndex, "\\x".getBytes());
        } else {
            statement.setBytes(paramIndex, Base64.getDecoder().decode(base64Encoded));
        }
    }
}
