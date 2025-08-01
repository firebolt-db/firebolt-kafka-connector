package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * A class that tries to convert the value from the kafka message to an array firebolt type
 */
public class ArrayDataTypeConverter extends CompositeDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Array array = convertToArray(statement.getConnection(), kafkaMessageColumnValue, fireboltColumn);
        statement.setArray(paramIndex, array);
    }

    private Array convertToArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Long> elements = (List) kafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(fireboltColumn);
        return connection.createArrayOf(typeName, elements.toArray());
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return "integer";
        }

        // add more data types
        return "string";
    }


}
