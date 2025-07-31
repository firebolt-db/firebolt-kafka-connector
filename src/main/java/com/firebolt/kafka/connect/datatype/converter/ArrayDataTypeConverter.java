package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;

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
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(fireboltColumn);
        if (CollectionUtils.isEmpty(elements)) {
            return connection.createArrayOf(typeName, elements.toArray());
        }

        // jdbc driver is not creating timestamps but array[integers] since the values are coming as ints
        if (typeName.equals("timestamp")) {
            if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT64) {
                return connection.createArrayOf(typeName, elements.stream().map(objectValue -> TimestampUtil.asTimestamp((Long) objectValue)).toArray());
            } else if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING) {
                return connection.createArrayOf("string", elements.toArray());
            }
        } else if (typeName.equals("numeric")) {
            if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
                return connection.createArrayOf("string", elements.toArray());
            }
        }

        return connection.createArrayOf(typeName, elements.toArray());
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return "integer";
        } else if (fireboltColumn.getDataType().equals("array(timestamp)")) {
            return "timestamp";
        } else if (fireboltColumn.getDataType().equals("array(numeric)")) {
            return "numeric";
        }

        // add more data types
        return "string";
    }


}
