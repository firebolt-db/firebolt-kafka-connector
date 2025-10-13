package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemaTextDataTypeConverter implements ColumnDataTypeConverter {
    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        statement.setString(paramIndex, String.valueOf(kafkaMessageColumnValue.getValue()));
    }
}


