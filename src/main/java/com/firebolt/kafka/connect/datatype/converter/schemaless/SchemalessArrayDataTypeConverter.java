package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.ArrayDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltByteaConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;

public class SchemalessArrayDataTypeConverter extends ArrayDataTypeConverter {

    private static final String DATE_ARRAY_TYPE_NAME = "date";
    private static final String TIMESTAMP_ARRAY_TYPE_NAME = "timestamp";
    private static final String TIMESTAMPTZ_ARRAY_TYPE_NAME = "timestamptz";
    private static final String BYTEA_ARRAY_TYPE_NAME = "bytea";
    private static final String REAL_TYPE_NAME = "real";

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
            if (kafkaMessageColumnValue.getValue() instanceof String) {
                return connection.createArrayOf("string", elements.stream().map(objectValue -> objectValue == null ? null : String.valueOf(objectValue)).toArray());
            }
        } else if (typeName.equals(REAL_TYPE_NAME)) {
            return createRealArray(connection, kafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals(BYTEA_ARRAY_TYPE_NAME)) {
            return createByteaArray(connection, kafkaMessageColumnValue, fireboltColumn);
        }

        return connection.createArrayOf(typeName, toObjectArray(elements));
    }

    private Array createRealArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

        // we need to set it as string as some setFloat is not working properly
        return connection.createArrayOf("string", elements.stream().map(object -> asReal(object, fireboltColumn)).toArray());
    }

    private String asReal(Object arrayElement, TableSchema.Column fireboltColumn) {
        if (arrayElement == null) {
            return null;
        }

        // integers are deserialized as longs
        if (arrayElement instanceof Long) {
            Long longValue = (Long) arrayElement;
            if (longValue >= Integer.MIN_VALUE || longValue <= Integer.MAX_VALUE) {
                return String.valueOf(longValue);
            }
        }

        // floating numbers are deserialized as Double
        if (arrayElement instanceof Double) {
            Double doubleValue = (Double) arrayElement;

            // only proceed if the value is in between the float ranges
            if (doubleValue >= -Float.MAX_VALUE || doubleValue <= Float.MAX_VALUE) {
                return String.valueOf(doubleValue);
            }
        }

        if (arrayElement instanceof String) {
            String s = (String) arrayElement;
            try {
                s.trim();
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a real due to NumberFormatException: " + e.getMessage());
            }
        }

        throw new ColumnConversionFailedException(
                fireboltColumn.getName(), fireboltColumn.getDataType(),
                "Cannot convert kafka message attribute to a real as the type does not match");
    }

    private Object[] toObjectArray(List<Object> elements) {
        Optional<Object> maybeFirst = elements.stream().filter(Objects::nonNull).findFirst();
        if (maybeFirst.isPresent() && maybeFirst.get().getClass() == ArrayList.class) {
            return elements.stream().map(element -> {
                if (element == null) {
                    return null;
                }
                return toObjectArray((List<Object>) element);
            }).toArray();
        }
        return elements.toArray();
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
            return BYTEA_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(boolean)")) {
            return "boolean";
        }

        // add more data types
        return "string";
    }

    private Array createByteaArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();

        // empty byte array will be serialized as empty string in kafka connect. In firebolt and empty byte is represented by \x
        return connection.createArrayOf(BYTEA_ARRAY_TYPE_NAME, elements.stream().map(objectValue -> objectValue == null ? null :asBytea(objectValue)).toArray());
    }

    private byte[] asBytea(Object o) {
        if (o instanceof String) {
            // empty array will be desrialized as ""
            return "".equals(o) ? "\\x".getBytes() : ((String) o).getBytes();
        }

        byte[] array = (o instanceof byte[]) ? (byte[]) o : ((ByteBuffer) o).array();
        return FireboltByteaConverter.convertFireboltBytea(array);
    }

    private Array createDateArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();
        return connection.createArrayOf(DATE_ARRAY_TYPE_NAME, elements.stream().map(this::asStringDate).toArray());
    }

    private Array createTimestampArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();
        return connection.createArrayOf("string", elements.stream().map(this::asStringTimestamp).toArray());
    }

    private Array createTimestamptzArray(Connection connection, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) kafkaMessageColumnValue.getValue();
        return connection.createArrayOf("string", elements.stream().map(this::asStringTimestamptz).toArray());
    }

    private String asStringTimestamp(Object arrayElement) {
        if (arrayElement == null) {
            return null;
        }

        if (arrayElement instanceof String) {
            return (String) arrayElement;
        } else if (arrayElement instanceof Number) {
            long millisFromEpoch =((Number) arrayElement).longValue();
            return TimestampUtil.asTimestamp(millisFromEpoch).toInstant().toString();
        }


        throw new ColumnConversionFailedException("","", "failed to convert string as timestamp");
    }

    private String asStringTimestamptz(Object arrayElement) {
        if (arrayElement == null) {
            return null;
        }

        if (arrayElement instanceof String && FireboltTimestamptzConverter.isValidTimestamptz((String) arrayElement)) {
            return (String) arrayElement;
        }

        if (arrayElement instanceof Number) {
            long millisFromEpoch = ((Number) arrayElement).longValue();
            OffsetDateTime offsetDateTime = TimestampUtil.asOffsetDateTime(millisFromEpoch);
            return offsetDateTime.toInstant().toString();
        }

        throw new ColumnConversionFailedException("","", "failed to convert string as timestamptz");
    }

    private Object asStringDate(Object arrayElement) {
        if (arrayElement == null) {
            return null;
        }

        if (arrayElement instanceof Number) {
            int numberOfDaysFromEpoch = ((Number) arrayElement).intValue();
            LocalDate localDate = LocalDate.ofEpochDay(numberOfDaysFromEpoch);
            return Date.valueOf(localDate);
        }

        if (arrayElement instanceof String && isIsoLocalDate((String) arrayElement)) {
            return arrayElement;
        }

        throw new ColumnConversionFailedException("","", "failed to convert string as date");
    }
}


