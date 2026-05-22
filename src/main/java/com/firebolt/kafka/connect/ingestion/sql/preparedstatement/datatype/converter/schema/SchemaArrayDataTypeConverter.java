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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

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

        if (isNestedArray(fireboltColumn)) {
            // Nested arrays land here regardless of inner type. Element-level
            // converters (timestamp/date/decimal/bytea) are not applied per
            // inner element today: the connector currently only supports
            // nested arrays of passthrough scalar types (integer/bigint/real
            // (non-FLOAT32)/double/boolean/text). Inner types that need
            // element-level conversion are rejected up front so callers get a
            // clear conversion error instead of malformed data downstream.
            if (requiresElementWiseConversion(fireboltColumn, schemaKafkaMessageColumnValue)) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(),
                        fireboltColumn.getDataType(),
                        "Nested arrays with element-level conversion (timestamp/date/decimal/bytea) are not supported");
            }
            return connection.createArrayOf(typeName, elements.stream().map(this::asNestedArrayElement).toArray());
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
        if (maybeFirst.isPresent() && maybeFirst.get() instanceof List) {
            return elements.stream().map(element -> {
                if (element == null) {
                    return null;
                }
                return toObjectArray((List<Object>) element);
            }).toArray();
        }
        return elements.toArray();
    }

    private Object asNestedArrayElement(Object element) {
        if (element == null) {
            return null;
        }
        if (element instanceof List) {
            return toObjectArray((List<Object>) element);
        }
        if (element instanceof Struct) {
            // Protobuf models nested arrays as `repeated WrapperMessage { repeated X values; }`.
            // Confluent's ProtobufConverter therefore surfaces nested arrays as a list of single-field
            // Connect Structs whose only field is the inner array. Unwrap that field here. The struct's
            // array field can also be null (e.g., absent repeated proto field with null value), in
            // which case we propagate null rather than NPE.
            List<Object> innerList = extractArrayField((Struct) element);
            return innerList == null ? null : toObjectArray(innerList);
        }
        throw new ColumnConversionFailedException("", "", "failed to convert nested array element of type " + element.getClass().getName());
    }

    private List<Object> extractArrayField(Struct struct) {
        for (Field field : struct.schema().fields()) {
            if (field.schema().type() == Schema.Type.ARRAY) {
                return (List<Object>) struct.get(field);
            }
        }
        throw new ColumnConversionFailedException("", "", "failed to convert nested array struct: no inner array field");
    }

    private boolean requiresElementWiseConversion(TableSchema.Column fireboltColumn, SchemaKafkaMessageColumnValue value) {
        String inner = innerNestedArrayType(fireboltColumn.getDataType());
        if (inner == null) {
            return false;
        }
        // Element-level conversion is required for these types because the inner
        // values arrive in formats (Long millis, ByteBuffer, ISO strings, ...)
        // that need translating before being shipped to the JDBC driver.
        switch (inner) {
            case "timestamp":
            case "timestamptz":
            case "date":
            case "numeric":
            case "bytea":
                return true;
            case "real":
                // FLOAT32 values must be re-stringified, see 1D path above.
                return value.getSchemaSubType() == Schema.Type.FLOAT32;
            default:
                return false;
        }
    }

    /** Returns the inner element type for `array(array(<inner>))`, else null. */
    private String innerNestedArrayType(String dataType) {
        if (dataType == null || !dataType.startsWith("array(array(") || !dataType.endsWith("))")) {
            return null;
        }
        return dataType.substring("array(array(".length(), dataType.length() - 2);
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        String dataType = fireboltColumn.getDataType();
        // For nested arrays, the JDBC driver expects the *inner* element type so it
        // can format `[[a,b],[c]]` as that type. Map the inner type the same way as
        // 1D arrays (text -> "string", bool -> "boolean", ...).
        String nestedInner = innerNestedArrayType(dataType);
        if (nestedInner != null) {
            return mapInnerToJdbcType(nestedInner);
        }
        if (dataType.startsWith("array(") && dataType.endsWith(")")) {
            return mapInnerToJdbcType(dataType.substring("array(".length(), dataType.length() - 1));
        }
        // add more data types
        return "string";
    }

    private String mapInnerToJdbcType(String inner) {
        switch (inner) {
            case "integer":
                return "integer";
            case "timestamp":
                return TIMESTAMP_ARRAY_TYPE_NAME;
            case "timestamptz":
                return TIMESTAMPTZ_ARRAY_TYPE_NAME;
            case "date":
                return DATE_ARRAY_TYPE_NAME;
            case "numeric":
                return "numeric";
            case "bigint":
                return "bigint";
            case "real":
                return "real";
            case "double":
                return "double";
            case "text":
                return "string";
            case "bytea":
                return BYTEA_ARRAY_TYPE_NAME;
            case "boolean":
                return "boolean";
            default:
                return "string";
        }
    }

    private boolean isNestedArray(TableSchema.Column fireboltColumn) {
        return innerNestedArrayType(fireboltColumn.getDataType()) != null;
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
           // ProtobufConverter maps google.protobuf.Timestamp elements to java.util.Date;
           // Avro / JSON Schema send Long (millis). Handle both.
           return connection.createArrayOf(TIMESTAMPTZ_ARRAY_TYPE_NAME, elements.stream().map(this::asOffsetDateTime).toArray());
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


    /** Converts a timestamptz array element (Long millis or java.util.Date) to OffsetDateTime. */
    private OffsetDateTime asOffsetDateTime(Object element) {
        if (element == null) return null;
        if (element instanceof java.util.Date) {
            return ((java.util.Date) element).toInstant().atOffset(ZoneOffset.UTC);
        }
        return TimestampUtil.asOffsetDateTime((Long) element);
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


