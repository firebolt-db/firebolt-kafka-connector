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
            // Any `array(array(...))` column lands here, including triple- and quadruple-nested
            // arrays. Element-level converters (timestamp/date/decimal/bytea) are not applied per
            // inner element today: the connector currently only supports nested arrays of
            // passthrough scalar types (integer/bigint/real (non-FLOAT32)/double/boolean/text).
            // Inner types that need element-level conversion are rejected up front so callers get
            // a clear conversion error instead of malformed data downstream.
            //
            // TODO: fold the element-level conversion logic (createTimestampArray /
            // createTimestamptzArray / createDateArray / createByteaArray / numeric stringify /
            // FLOAT32 stringify) into the recursive deep-unwrap below so nested arrays of those
            // types can also be ingested.
            if (requiresElementWiseConversion(fireboltColumn, schemaKafkaMessageColumnValue)) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(),
                        fireboltColumn.getDataType(),
                        "Nested arrays with element-level conversion (timestamp/date/decimal/bytea) are not supported");
            }
            return connection.createArrayOf(typeName, deepUnwrap(elements, arrayNestingDepth(fireboltColumn.getDataType()) - 1));
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

    /**
     * Recursively unwraps a nested array of arbitrary depth into a Java {@code Object[]} hierarchy
     * suitable for {@link Connection#createArrayOf(String, Object[])}. Handles two surface shapes
     * at every level:
     *
     * <ul>
     *   <li>List (Avro / direct nested arrays).</li>
     *   <li>Connect {@link Struct} wrapping a single repeated field (Protobuf models nested
     *       arrays as {@code repeated WrapperMessage { repeated X values; }} at every level, so a
     *       triple-nested protobuf array surfaces as List&lt;Struct&lt;List&lt;Struct&lt;List&lt;X&gt;&gt;&gt;&gt;&gt;).</li>
     * </ul>
     *
     * The {@code remainingDepth} argument is the number of array levels still to traverse: at
     * depth 0 we have reached the leaf scalar layer and elements are returned as-is. A null inner
     * List from a wrapper Struct propagates as null instead of NPE'ing.
     */
    private Object[] deepUnwrap(List<Object> elements, int remainingDepth) {
        if (remainingDepth <= 0) {
            return elements.toArray();
        }
        return elements.stream().map(element -> deepUnwrapElement(element, remainingDepth)).toArray();
    }

    private Object deepUnwrapElement(Object element, int remainingDepth) {
        if (element == null) {
            return null;
        }
        if (remainingDepth <= 0) {
            return element;
        }
        if (element instanceof List) {
            return deepUnwrap((List<Object>) element, remainingDepth - 1);
        }
        if (element instanceof Struct) {
            List<Object> innerList = extractArrayField((Struct) element);
            return innerList == null ? null : deepUnwrap(innerList, remainingDepth - 1);
        }
        throw new ColumnConversionFailedException(
                "", "", "failed to convert nested array element of type " + element.getClass().getName());
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
        String leaf = leafScalarType(fireboltColumn.getDataType());
        if (leaf == null) {
            return false;
        }
        // Element-level conversion is required for these types because the inner
        // values arrive in formats (Long millis, ByteBuffer, ISO strings, ...)
        // that need translating before being shipped to the JDBC driver.
        switch (leaf) {
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

    /** Returns the innermost scalar type for an `array(array(...(<scalar>)))` column, peeling off
     *  every `array(...)` wrapper. Returns the dataType unchanged for non-array columns. */
    private String leafScalarType(String dataType) {
        String inner = dataType;
        while (inner != null && inner.startsWith("array(") && inner.endsWith(")")) {
            inner = inner.substring("array(".length(), inner.length() - 1);
        }
        return inner;
    }

    /** Counts how many array levels wrap the leaf scalar type. */
    private int arrayNestingDepth(String dataType) {
        int depth = 0;
        String inner = dataType;
        while (inner != null && inner.startsWith("array(") && inner.endsWith(")")) {
            depth++;
            inner = inner.substring("array(".length(), inner.length() - 1);
        }
        return depth;
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        // The JDBC driver expects the leaf scalar type name regardless of nesting depth -- it
        // formats nested arrays recursively using a single base type, so `array(integer)`,
        // `array(array(integer))`, and `array(array(array(integer)))` all resolve to "integer".
        return mapInnerToJdbcType(leafScalarType(fireboltColumn.getDataType()));
    }

    private String mapInnerToJdbcType(String inner) {
        if (inner == null) {
            return "string";
        }
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
        return arrayNestingDepth(fireboltColumn.getDataType()) >= 2;
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


