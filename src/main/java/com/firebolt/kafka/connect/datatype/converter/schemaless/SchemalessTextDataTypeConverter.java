package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SchemalessTextDataTypeConverter implements ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue> {
    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        statement.setString(paramIndex, String.valueOf(schemalessKafkaMessageColumnValue.getValue()));
    }
}


