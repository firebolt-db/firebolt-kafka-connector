package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.ByteaDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessByteaDataTypeConverter extends ByteaDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();
        if (value instanceof String) {
            statement.setString(paramIndex, (String) value);
            return;
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }
}


