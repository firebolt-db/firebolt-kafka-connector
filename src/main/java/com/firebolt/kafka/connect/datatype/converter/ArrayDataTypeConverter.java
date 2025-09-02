package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;

/**
 * A class that tries to convert the value from the kafka message to an array firebolt type
 */
public class ArrayDataTypeConverter extends CompositeDataTypeConverter {

    private static final String DATE_ARRAY_TYPE_NAME = "date";
    private static final String TIMESTAMP_ARRAY_TYPE_NAME = "timestamp";
    private static final String TIMESTAMPTZ_ARRAY_TYPE_NAME = "timestamptz";

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException, ColumnConversionFailedException {
        Array array = convertToArray(statement.getConnection(), kafkaMessageColumnValue, fireboltColumn);
        statement.setArray(paramIndex, array);
    }

    private Array convertToArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException, ColumnConversionFailedException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(fireboltColumn);
        if (CollectionUtils.isEmpty(elements)) {
            return connection.createArrayOf(typeName, elements.toArray());
        }

        // jdbc driver is not creating timestamps but array[integers] since the values are coming as ints
        if (typeName.equals(TIMESTAMP_ARRAY_TYPE_NAME)) {
            return createTimestampArray(connection, kafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals(TIMESTAMPTZ_ARRAY_TYPE_NAME)) {
            return createTimestamptzArray(connection, kafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals(DATE_ARRAY_TYPE_NAME)) {
            return createDateArray(connection, kafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals("numeric")) {
            if (kafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING ||
                    kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.BYTES) {
                return connection.createArrayOf("string", elements.stream().map(objectValue -> objectValue == null ? null : String.valueOf(objectValue)).toArray());
            }
        } else if (typeName.equals("real")) {
            if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.FLOAT32) {
                return connection.createArrayOf(typeName, elements.stream().map(objectValue -> objectValue == null ? null : String.valueOf(objectValue)).toArray());
            }
        } else if (typeName.equals("bytea")) {
            // empty byte array will be serialized as empty string in kafka connect. In firebolt and empty byte is represented by \x
            return connection.createArrayOf(typeName, elements.stream().map(objectValue -> objectValue == null ? null : "".equals(objectValue) ? "\\x".getBytes() : Base64.getDecoder().decode(String.valueOf(objectValue))).toArray());
        }

        return connection.createArrayOf(typeName, elements.toArray());
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return "integer";
        } else if (fireboltColumn.getDataType().equals("array(timestamp)")) {
            return TIMESTAMP_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(timestamptz)")) {
            return TIMESTAMPTZ_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(date)")) {
            return DATE_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(numeric)")) {
            return "numeric";
        } else if (fireboltColumn.getDataType().equals("array(bigint)")) {
            return "bigint";
        } else if (fireboltColumn.getDataType().equals("array(real)")) {
            return "real";
        } else if (fireboltColumn.getDataType().equals("array(double)")) {
            return "double";
        } else if (fireboltColumn.getDataType().equals("array(text)")) {
            return "string";
        } else if (fireboltColumn.getDataType().equals("array(bytea)")) {
            return "bytea";
        } else if (fireboltColumn.getDataType().equals("array(boolean)")) {
            return "boolean";
        }

        // add more data types
        return "string";
    }

    private Array createDateArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

        if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING || kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT32) {
            return connection.createArrayOf(DATE_ARRAY_TYPE_NAME, elements.stream().map(this::asStringDate).toArray());
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Failed to convert the date array to firebolt column");
    }

    private Array createTimestampArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

        if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT64) {
            return connection.createArrayOf(TIMESTAMP_ARRAY_TYPE_NAME, elements.stream().map(this::asStringTimestamp).toArray());
        } else if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING) {
            return connection.createArrayOf("string", elements.toArray());
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Failed to convert the timestamp array to firebolt column");
    }

   private Array createTimestamptzArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

       if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT64) {
           return connection.createArrayOf(TIMESTAMPTZ_ARRAY_TYPE_NAME, elements.stream().map(objectValue -> TimestampUtil.asOffsetDateTime((Long) objectValue)).toArray());
       } else if (kafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING) {
           return connection.createArrayOf("string", elements.stream().map(this::asStringTimestamptz).toArray());
       }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Failed to convert the timestamptz array to firebolt column");
    }

    private Object asStringTimestamp(Object arrayElement) {
        if (arrayElement == null) {
            return null;
        }

        if (arrayElement instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) arrayElement).getTime());
        } else if (arrayElement instanceof Long) {
            return TimestampUtil.asTimestamp((Long) arrayElement);
        }


        throw new ColumnConversionFailedException("","", "failed to convert string as timestamp");
    }

    private Object asStringTimestamptz(Object arrayElement) {
        if (arrayElement == null) {
            return null;
        }

        if (arrayElement instanceof String && FireboltTimestamptzConverter.isValidTimestamptz((String) arrayElement)) {
            return arrayElement;
        }

        throw new ColumnConversionFailedException("","", "failed to convert string as timestamptz");
    }

    private Object asStringDate(Object arrayElement) {
        if (arrayElement == null) {
            return null;
        }

        if (arrayElement instanceof java.util.Date) {
            return new Date(((java.util.Date) arrayElement).getTime());
        }

        if (arrayElement instanceof String && isIsoLocalDate((String) arrayElement)) {
            return arrayElement;
        }

        throw new ColumnConversionFailedException("","", "failed to convert string as date");
    }

}
