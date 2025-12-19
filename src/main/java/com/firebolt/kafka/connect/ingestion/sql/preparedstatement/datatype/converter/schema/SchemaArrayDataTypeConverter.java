package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.CompositeDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.FireboltByteaConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;

/**
 * A class that tries to convert the value from the kafka message to an array firebolt type
 */
public class SchemaArrayDataTypeConverter extends CompositeDataTypeConverter<SchemaKafkaMessageColumnValue> {

    private static final String DATE_ARRAY_TYPE_NAME = "date";
    private static final String TIMESTAMP_ARRAY_TYPE_NAME = "timestamp";
    private static final String TIMESTAMPTZ_ARRAY_TYPE_NAME = "timestamptz";
    private static final String BYTEA_ARRAY_TYPE_NAME = "bytea";

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException, ColumnConversionFailedException {
        Array array = convertToArray(statement.getConnection(), schemaKafkaMessageColumnValue, fireboltColumn);
        statement.setArray(paramIndex, array);
    }

    private Array convertToArray(Connection connection, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException, ColumnConversionFailedException {
        List<Object> elements = (List) schemaKafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(fireboltColumn);
        if (CollectionUtils.isEmpty(elements)) {
            return connection.createArrayOf(typeName, elements.toArray());
        }

        // jdbc driver is not creating timestamps but array[integers] since the values are coming as ints
        if (typeName.equals(TIMESTAMP_ARRAY_TYPE_NAME)) {
            return createTimestampArray(connection, schemaKafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals(TIMESTAMPTZ_ARRAY_TYPE_NAME)) {
            return createTimestamptzArray(connection, schemaKafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals(DATE_ARRAY_TYPE_NAME)) {
            return createDateArray(connection, schemaKafkaMessageColumnValue, fireboltColumn);
        } else if (typeName.equals("numeric")) {
            if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING ||
                    schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.BYTES) {
                return connection.createArrayOf("string", elements.stream().map(objectValue -> objectValue == null ? null : String.valueOf(objectValue)).toArray());
            }
        } else if (typeName.equals("real")) {
            if (schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.FLOAT32) {
                return connection.createArrayOf(typeName, elements.stream().map(objectValue -> objectValue == null ? null : String.valueOf(objectValue)).toArray());
            }
        } else if (typeName.equals(BYTEA_ARRAY_TYPE_NAME)) {
            return createByteaArray(connection, schemaKafkaMessageColumnValue, fireboltColumn);
        }

        return connection.createArrayOf(typeName, toObjectArray(elements));
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

    private Array createByteaArray(Connection connection, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) schemaKafkaMessageColumnValue.getValue();

        // empty byte array will be serialized as empty string in kafka connect. In firebolt and empty byte is represented by \x
        return connection.createArrayOf(BYTEA_ARRAY_TYPE_NAME, elements.stream().map(objectValue -> objectValue == null ? null :asBytea(objectValue)).toArray());

    }

    private byte[] asBytea(Object o) {
        if (o instanceof String) {
            return ((String) o).getBytes();
        }

        byte[] array = (o instanceof byte[]) ? (byte[]) o : ((ByteBuffer) o).array();
        return FireboltByteaConverter.convertFireboltBytea(array);
    }

    private Array createDateArray(Connection connection, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) schemaKafkaMessageColumnValue.getValue();

        if (schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING || schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT32) {
            return connection.createArrayOf(DATE_ARRAY_TYPE_NAME, elements.stream().map(this::asStringDate).toArray());
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Failed to convert the date array to firebolt column");
    }

    private Array createTimestampArray(Connection connection, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) schemaKafkaMessageColumnValue.getValue();

        if (schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT64) {
            return connection.createArrayOf(TIMESTAMP_ARRAY_TYPE_NAME, elements.stream().map(this::asStringTimestamp).toArray());
        } else if (schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING) {
            return connection.createArrayOf("string", elements.toArray());
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Failed to convert the timestamp array to firebolt column");
    }

   private Array createTimestamptzArray(Connection connection, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        List<Object> elements = (List) schemaKafkaMessageColumnValue.getValue();

       if (schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.INT64) {
           return connection.createArrayOf(TIMESTAMPTZ_ARRAY_TYPE_NAME, elements.stream().map(objectValue -> TimestampUtil.asOffsetDateTime((Long) objectValue)).toArray());
       } else if (schemaKafkaMessageColumnValue.getSchemaSubType() == Schema.Type.STRING) {
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


