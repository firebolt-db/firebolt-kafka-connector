package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaArrayDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    @Mock
    private Connection mockConnection;

    @Mock
    private Array mockArray;

    private SchemaArrayDataTypeConverter converter;
    private TableSchema.Column integerArrayColumn;
    private TableSchema.Column textArrayColumn;
    private TableSchema.Column timestampArrayColumn;
    private TableSchema.Column numericArrayColumn;
    private TableSchema.Column dateArrayColumn;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaArrayDataTypeConverter();
        integerArrayColumn = new TableSchema.Column("test_column", "array(integer)", 2003, true);
        textArrayColumn = new TableSchema.Column("test_column", "array(text)", 2003, true);
        timestampArrayColumn = new TableSchema.Column("test_column", "array(timestamp)", 2003, true);
        numericArrayColumn = new TableSchema.Column("test_column", "array(numeric)", 2003, true);
        dateArrayColumn = new TableSchema.Column("test_column", "array(date)", 2003, true);
        
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        when(mockConnection.createArrayOf(any(String.class), any(Object[].class))).thenReturn(mockArray);
    }

    @ParameterizedTest
    @CsvSource({
        "1, 'Single positive value'",
        "-1, 'Single negative value'",
        "0, 'Single zero value'",
        "2147483647, 'Integer.MAX_VALUE'",
        "-2147483648, 'Integer.MIN_VALUE'"
    })
    void testConvertAndSetWithSingleValueArrays(long value, String description) throws SQLException {
        List<Long> arrayValues = Arrays.asList(value);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithMultipleIntegerValues() throws SQLException {
        List<Long> arrayValues = Arrays.asList(
                (long) Integer.MIN_VALUE, 
                -1000L, 
                -1L, 
                0L, 
                1L, 
                1000L, 
                (long) Integer.MAX_VALUE
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedIntegerArrayFromProtobufWrapperStructs() throws SQLException {
        org.apache.kafka.connect.data.Schema nestedSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build())
                .build();
        org.apache.kafka.connect.data.Struct first = new org.apache.kafka.connect.data.Struct(nestedSchema)
                .put("values", Arrays.asList(1, 2));
        org.apache.kafka.connect.data.Struct second = new org.apache.kafka.connect.data.Struct(nestedSchema)
                .put("values", Arrays.asList(3, null, 4));
        List<Object> arrayValues = Arrays.asList(first, second);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column nestedArrayColumn = new TableSchema.Column("test_column", "array(array(integer))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, nestedArrayColumn);

        Object[] expected = new Object[] {
                new Object[] {1, 2},
                new Object[] {3, null, 4}
        };
        verify(mockConnection).createArrayOf(eq("integer"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedTextArrayFromProtobufWrapperStructs() throws SQLException {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
                .build();
        org.apache.kafka.connect.data.Struct first = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList("a", "b"));
        org.apache.kafka.connect.data.Struct second = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList("c", null, "d"));
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(first, second))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column nestedTextColumn = new TableSchema.Column("c", "array(array(text))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, nestedTextColumn);

        Object[] expected = new Object[] {
                new Object[] {"a", "b"},
                new Object[] {"c", null, "d"}
        };
        verify(mockConnection).createArrayOf(eq("string"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedDoubleArrayFromProtobufWrapperStructs() throws SQLException {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build())
                .build();
        org.apache.kafka.connect.data.Struct first = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList(1.1, 2.2));
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(first))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column nestedDoubleColumn = new TableSchema.Column("c", "array(array(double))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, nestedDoubleColumn);

        Object[] expected = new Object[] { new Object[] {1.1, 2.2} };
        verify(mockConnection).createArrayOf(eq("double"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedBooleanArrayFromProtobufWrapperStructs() throws SQLException {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).build())
                .build();
        org.apache.kafka.connect.data.Struct first = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList(true, false, null));
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(first))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column nestedBooleanColumn = new TableSchema.Column("c", "array(array(boolean))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, nestedBooleanColumn);

        Object[] expected = new Object[] { new Object[] {true, false, null} };
        verify(mockConnection).createArrayOf(eq("boolean"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedBigintArrayFromProtobufWrapperStructs() throws SQLException {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.INT64_SCHEMA).build())
                .build();
        org.apache.kafka.connect.data.Struct first = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList(1L, 2L));
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(first))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(bigint))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, col);

        Object[] expected = new Object[] { new Object[] {1L, 2L} };
        verify(mockConnection).createArrayOf(eq("bigint"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedRealArrayFromProtobufWrapperStructs() throws SQLException {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
                .build();
        org.apache.kafka.connect.data.Struct first = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList(1.1f, 2.2f));
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(first))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(real))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, col);
        Object[] expected = new Object[] { new Object[] {1.1f, 2.2f} };
        verify(mockConnection).createArrayOf(eq("real"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedArrayProtobufWrapperStructWithNullInnerArrayDoesNotNPE() throws SQLException {
        // The struct has a nullable repeated field whose value is explicitly null, mimicking
        // the runtime shape produced by ProtobufConverter when a wrapper message is sent with
        // an absent inner repeated field. Previously this caused a NullPointerException
        // because asNestedArrayElement -> toObjectArray was called on a null inner List.
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build())
                .build();
        org.apache.kafka.connect.data.Struct nullInner = new org.apache.kafka.connect.data.Struct(wrapperSchema);
        nullInner.put("values", null);
        org.apache.kafka.connect.data.Struct populated = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList(7, 8));
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(nullInner, populated))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column nestedArrayColumn = new TableSchema.Column("c", "array(array(integer))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, nestedArrayColumn);

        Object[] expected = new Object[] { null, new Object[] {7, 8} };
        verify(mockConnection).createArrayOf(eq("integer"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedArrayWithNullOuterElementDoesNotNPE() throws SQLException {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("values", org.apache.kafka.connect.data.SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build())
                .optional()
                .build();
        org.apache.kafka.connect.data.Struct populated = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("values", Arrays.asList(1, 2));
        // Outer element is null
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(populated, null))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(integer))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, col);

        Object[] expected = new Object[] { new Object[] {1, 2}, null };
        verify(mockConnection).createArrayOf(eq("integer"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedAvroLikeListOfListIntegerArray() throws SQLException {
        // Avro nested arrays surface as List<List<X>> directly (no Struct wrapper).
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5)))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.ARRAY)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(integer))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, col);

        Object[] expected = new Object[] { new Object[] {1, 2}, new Object[] {3, 4, 5} };
        verify(mockConnection).createArrayOf(eq("integer"), argThat(actual -> Arrays.deepEquals(expected, actual)));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNestedTimestampArrayThrows() {
        // Element-level conversion is required for timestamp; the nested path is not supported.
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(Arrays.asList(1L, 2L)))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.ARRAY)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(timestamp))", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, col));
    }

    @Test
    void testConvertAndSetWithNestedDateArrayThrows() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(Arrays.asList("2024-01-01")))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.ARRAY)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(date))", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, col));
    }

    @Test
    void testConvertAndSetWithNestedNumericArrayThrows() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(Arrays.asList("1.0")))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.ARRAY)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(numeric))", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, col));
    }

    @Test
    void testConvertAndSetWithNestedRealFloat32SchemaThrows() {
        // FLOAT32 needs re-stringification, so nested float32 path is rejected.
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(Arrays.asList(1.0f)))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.FLOAT32)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(real))", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, col));
    }

    @Test
    void testConvertAndSetWithNestedArrayUnsupportedElementTypeThrows() {
        // Outer list contains an opaque Object (neither List nor Struct). Should fail with a
        // typed ColumnConversionFailedException, not an unguarded ClassCastException/NPE.
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(new Object()))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(integer))", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, col));
    }

    @Test
    void testConvertAndSetWithNestedArrayStructWithoutInnerArrayFieldThrows() {
        org.apache.kafka.connect.data.Schema wrapperSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("notAnArray", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        org.apache.kafka.connect.data.Struct s = new org.apache.kafka.connect.data.Struct(wrapperSchema)
                .put("notAnArray", "x");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Arrays.asList(s))
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRUCT)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(integer))", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, col));
    }

    @Test
    void testConvertAndSetWithEmptyNestedArray() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(new ArrayList<>())
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.ARRAY)
                .build();
        TableSchema.Column col = new TableSchema.Column("c", "array(array(integer))", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, col);

        verify(mockConnection).createArrayOf(eq("integer"), eq(new Object[]{}));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyArray() throws SQLException {
        List<Long> emptyArray = new ArrayList<>();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(emptyArray)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithArrayContainingZerosAndNulls() throws SQLException {
        List<Long> arrayValues = Arrays.asList(0L, 0L, 0L, null, null);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithLargeArray() throws SQLException {
        List<Long> largeArray = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeArray.add((long) i);
        }
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(largeArray)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(largeArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTextArrayType() throws SQLException {
        List<String> textValues = Arrays.asList("hello", "world", "text array test");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(textValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, textArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(textValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTextArrayTypeVariousStrings() throws SQLException {
        List<String> textValues = Arrays.asList(
            "simple text", 
            "text with spaces", 
            "123 numeric string", 
            "special!@#$%chars",
            "unicode: café naïve",
            "", // empty string
            "text\nwith\nnewlines",
            "very long text that might be used to test performance and edge cases with larger string content"
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(textValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, textArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(textValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithBigIntArrayType() throws SQLException {
        List<Long> bigIntValues = Arrays.asList(1L, 2L, 3L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(bigIntValues)
                .build();

        TableSchema.Column bigIntArrayColumn = new TableSchema.Column("test_column", "array(bigint)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, bigIntArrayColumn);

        verify(mockConnection).createArrayOf(eq("bigint"), eq(bigIntValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithRealArrayType() throws SQLException {
        List<Float> realValues = Arrays.asList(1.5f, 2.7f, 3.14f);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(realValues)
                .build();

        TableSchema.Column realArrayColumn = new TableSchema.Column("test_column", "array(real)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, realArrayColumn);

        verify(mockConnection).createArrayOf(eq("real"), eq(realValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithRealArrayTypeFloat32Schema() throws SQLException {
        List<Float> realValues = Arrays.asList(1.5f, 2.7f, 3.14f, null, 0.0f);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(realValues)
                .schemaSubType(Schema.Type.FLOAT32)
                .build();

        TableSchema.Column realArrayColumn = new TableSchema.Column("test_column", "array(real)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, realArrayColumn);

        // For FLOAT32 schema, values should be converted to strings
        Object[] expectedValues = realValues.stream()
                .map(value -> value == null ? null : String.valueOf(value))
                .toArray();

        verify(mockConnection).createArrayOf(eq("real"), eq(expectedValues));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithDoubleArrayType() throws SQLException {
        List<Double> doubleValues = Arrays.asList(1.5, 2.7, 3.14159265359);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(doubleValues)
                .build();

        TableSchema.Column doubleArrayColumn = new TableSchema.Column("test_column", "array(double)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, doubleArrayColumn);

        verify(mockConnection).createArrayOf(eq("double"), eq(doubleValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithDateArrayTypeStringSchemaValidStrings() throws SQLException {
        List<String> dateValues = Arrays.asList("2023-01-01", "2024-12-31", null, "2000-02-29");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(dateValues)
                .schemaSubType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn);

        verify(mockConnection).createArrayOf(eq("date"), eq(dateValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithDateArrayTypeStringSchemaUtilDates() throws SQLException {
        java.sql.Date d1 = java.sql.Date.valueOf("2023-01-01");
        java.sql.Date d2 = java.sql.Date.valueOf("2020-02-29");
        java.util.Date u1 = new java.util.Date(d1.getTime());
        java.util.Date u2 = new java.util.Date(d2.getTime());

        List<Object> values = Arrays.asList(u1, null, u2);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(values)
                .schemaSubType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn);

        Object[] expected = new Object[]{java.sql.Date.valueOf("2023-01-01"), null, java.sql.Date.valueOf("2020-02-29")};
        verify(mockConnection).createArrayOf(eq("date"), eq(expected));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithDateArrayTypeStringSchemaMixedValues() throws SQLException {
        java.sql.Date d2 = java.sql.Date.valueOf("2020-02-29");
        java.util.Date u2 = new java.util.Date(d2.getTime());
        List<Object> values = Arrays.asList("2023-01-01", u2, null);

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(values)
                .schemaSubType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn);

        Object[] expected = new Object[]{"2023-01-01", java.sql.Date.valueOf("2020-02-29"), null};
        verify(mockConnection).createArrayOf(eq("date"), eq(expected));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyDateArray() throws SQLException {
        List<Object> emptyArray = new ArrayList<>();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(emptyArray)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn);

        verify(mockConnection).createArrayOf(eq("date"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithDateArrayInvalidStringThrows() {
        List<String> badValues = Arrays.asList("2024-1-02", "abc");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(badValues)
                .schemaSubType(Schema.Type.STRING)
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
            converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn));
    }

    @Test
    void testConvertAndSetWithDateArrayUnsupportedSchemaTypeThrows() {
        List<String> dateValues = Arrays.asList("2023-01-01");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(dateValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
            converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn));
    }

    @Test
    void testConvertAndSetWithDateArrayWithInt32() throws SQLException {
        java.sql.Date d2 = java.sql.Date.valueOf("2023-01-01");
        java.util.Date u2 = new java.util.Date(d2.getTime());
        List<Object> dateValues = Arrays.asList(u2);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(dateValues)
                .schemaSubType(Schema.Type.INT32)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, dateArrayColumn);
        Object[] expected = new Object[]{java.sql.Date.valueOf("2023-01-01")};
        verify(mockConnection).createArrayOf(eq("date"), eq(expected));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithBooleanArrayType() throws SQLException {
        List<Boolean> booleanValues = Arrays.asList(true, false, true, null, false);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(booleanValues)
                .build();

        TableSchema.Column booleanArrayColumn = new TableSchema.Column("test_column", "array(boolean)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, booleanArrayColumn);

        verify(mockConnection).createArrayOf(eq("boolean"), eq(booleanValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithUnsupportedArrayTypeDefaultsToString() throws SQLException {
        TableSchema.Column unsupportedColumn = new TableSchema.Column("test_column", "unsupported_array_type", 2003, true);
        List<Long> arrayValues = Arrays.asList(1L, 2L, 3L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, unsupportedColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @ParameterizedTest
    @CsvSource({
        "0",                    // Epoch
        "1000",                 // 1 second in millis
        "1609459200000",        // 2021-01-01 00:00:00 UTC in millis
        "10000000000000",       // Threshold value (treated as millis)
        "10000000000001"        // Just above threshold (treated as micros)
    })
    void testConvertAndSetWithTimestampArrayInt64Schema(long timestampValue) throws SQLException {
        List<Long> timestampValues = Arrays.asList(timestampValue, timestampValue + 1000);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestampArrayColumn);

        // Verify that TimestampUtil.asTimestamp() is called for each element
        Timestamp[] expectedTimestamps = timestampValues.stream()
                .map(TimestampUtil::asTimestamp)
                .toArray(Timestamp[]::new);

        verify(mockConnection).createArrayOf(eq("timestamp"), eq(expectedTimestamps));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestampArrayStringSchema() throws SQLException {
        List<String> timestampStrings = Arrays.asList("2021-01-01 00:00:00", "2024-12-31 23:59:59");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampStrings)
                .schemaSubType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestampArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(timestampStrings.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyTimestampArray() throws SQLException {
        List<Long> emptyArray = new ArrayList<>();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(emptyArray)
                .schemaSubType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestampArrayColumn);

        verify(mockConnection).createArrayOf(eq("timestamp"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestampArrayContainingNulls() throws SQLException {
        List<Long> timestampValues = Arrays.asList(1609459200000L, null, 1609459260000L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestampArrayColumn);

        // Verify that TimestampUtil.asTimestamp() is called for each element (including null)
        Timestamp[] expectedTimestamps = new Timestamp[]{
            TimestampUtil.asTimestamp(1609459200000L),
            TimestampUtil.asTimestamp(null),
            TimestampUtil.asTimestamp(1609459260000L)
        };

        verify(mockConnection).createArrayOf(eq("timestamp"), eq(expectedTimestamps));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithMixedTimestampValues() throws SQLException {
        // Test with millisecond and microsecond values mixed
        List<Long> timestampValues = Arrays.asList(
                1000L,              // Milliseconds
                10000000000001L,    // Microseconds (above threshold)
                1609459200000L      // Milliseconds
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestampArrayColumn);

        Timestamp[] expectedTimestamps = timestampValues.stream()
                .map(TimestampUtil::asTimestamp)
                .toArray(Timestamp[]::new);

        verify(mockConnection).createArrayOf(eq("timestamp"), eq(expectedTimestamps));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestampArrayInt64SchemaUsingDateValues() throws SQLException {
        // Values as java.util.Date should be converted to java.sql.Timestamp
        java.util.Date d1 = new java.util.Date(1700000000000L); // 2023-11-14T22:13:20Z approx
        java.util.Date d2 = new java.util.Date(1700003600000L);
        List<java.util.Date> timestampValues = Arrays.asList(d1, null, d2);

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestampArrayColumn);

        Timestamp[] expected = new Timestamp[]{
                new Timestamp(d1.getTime()),
                null,
                new Timestamp(d2.getTime())
        };
        verify(mockConnection).createArrayOf(eq("timestamp"), eq(expected));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestampArrayUnsupportedSubtypeThrows() {
        List<Integer> values = Arrays.asList(1, 2, 3);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(values)
                .schemaSubType(Schema.Type.INT32)
                .build();

        TableSchema.Column tsArrayColumn = new TableSchema.Column("test_column", "array(timestamp)", 2003, true);

        org.junit.jupiter.api.Assertions.assertThrows(
                ColumnConversionFailedException.class,
                () -> converter.convertAndSet(mockStatement, 1, kafkaValue, tsArrayColumn)
        );
    }

    @ParameterizedTest
    @CsvSource({
        "0",                    // Epoch
        "1000",                 // 1 second in millis
        "1609459200000",        // 2021-01-01 00:00:00 UTC in millis
        "10000000000000",       // Threshold value (treated as millis)
        "10000000000001"        // Just above threshold (treated as micros)
    })
    void testConvertAndSetWithTimestamptzArrayInt64Schema(long timestampValue) throws SQLException {
        List<Long> timestampValues = Arrays.asList(timestampValue, timestampValue + 1000);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        TableSchema.Column timestamptzArrayColumn = new TableSchema.Column("test_column", "array(timestamptz)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestamptzArrayColumn);

        // Verify values converted to OffsetDateTime via TimestampUtil
        Object[] expected = timestampValues.stream()
                .map(TimestampUtil::asOffsetDateTime)
                .toArray();
        verify(mockConnection).createArrayOf(eq("timestamptz"), eq(expected));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestamptzArrayStringSchema() throws SQLException {
        // Must be valid timestamptz strings
        List<String> timestampStrings = Arrays.asList("2021-01-01 00:00:00+00:00", "2024-12-31T23:59:59Z");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampStrings)
                .schemaSubType(Schema.Type.STRING)
                .build();

        TableSchema.Column timestamptzArrayColumn = new TableSchema.Column("test_column", "array(timestamptz)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestamptzArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(timestampStrings.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyTimestamptzArray() throws SQLException {
        List<Long> emptyArray = new ArrayList<>();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(emptyArray)
                .schemaSubType(Schema.Type.INT64)
                .build();

        TableSchema.Column timestamptzArrayColumn = new TableSchema.Column("test_column", "array(timestamptz)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestamptzArrayColumn);

        verify(mockConnection).createArrayOf(eq("timestamptz"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestamptzArrayContainingNulls() throws SQLException {
        List<Long> timestampValues = Arrays.asList(1609459200000L, null, 1609459260000L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        TableSchema.Column timestamptzArrayColumn = new TableSchema.Column("test_column", "array(timestamptz)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestamptzArrayColumn);

        Object[] expected = new Object[] {
                TimestampUtil.asOffsetDateTime(1609459200000L),
                null,
                TimestampUtil.asOffsetDateTime(1609459260000L)
        };
        verify(mockConnection).createArrayOf(eq("timestamptz"), eq(expected));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithMixedTimestamptzValues() throws SQLException {
        // Test with millisecond and microsecond values mixed
        List<Long> timestampValues = Arrays.asList(
                1000L,              // Milliseconds
                10000000000001L,    // Microseconds (above threshold)
                1609459200000L      // Milliseconds
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValues)
                .schemaSubType(Schema.Type.INT64)
                .build();

        TableSchema.Column timestamptzArrayColumn = new TableSchema.Column("test_column", "array(timestamptz)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, timestamptzArrayColumn);

        // Verify that createArrayOf is called with timestamptz type and any array of objects
        verify(mockConnection).createArrayOf(eq("timestamptz"), any(Object[].class));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTimestamptzArrayStringSchemaInvalidThrows() {
        List<String> invalidStrings = Arrays.asList("2021-01-01 00:00:00", "invalid");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(invalidStrings)
                .schemaSubType(Schema.Type.STRING)
                .build();

        TableSchema.Column timestamptzArrayColumn = new TableSchema.Column("test_column", "array(timestamptz)", 2003, true);

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, timestamptzArrayColumn));
    }

    @ParameterizedTest
    @CsvSource({
        "'123.45'",
        "'0.00'",
        "'-123.45'",
        "'999999.99'",
        "'0.01'",
        "'1000000.00'",
        "'123456789.123456789'",
        "'0'",
        "'-0.001'",
        "'1.23E-10'",
        "'12345678901234567890123456789.123456789'"
    })
    void testConvertAndSetWithNumericArrayStringSchema(String decimalString) throws SQLException {
        List<String> decimalValues = Arrays.asList(decimalString, "456.78", "0.99");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(decimalValues)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(decimalValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyNumericArray() throws SQLException {
        List<String> emptyArray = new ArrayList<>();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(emptyArray)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("numeric"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNumericArrayContainingNulls() throws SQLException {
        List<String> decimalValues = Arrays.asList("123.45", null, "456.78");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(decimalValues)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(decimalValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithLargeNumericArray() throws SQLException {
        List<String> largeArray = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeArray.add(String.valueOf(i) + ".99");
        }
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(largeArray)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(largeArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNumericArrayMixedPrecision() throws SQLException {
        List<String> mixedPrecisionValues = Arrays.asList(
            "123.45",           // 2 decimal places
            "123.456",          // 3 decimal places
            "123.4567",         // 4 decimal places
            "123.456789012345678901234567890", // Very high precision
            "0",                // Integer
            "-123.45"           // Negative
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(mixedPrecisionValues)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(mixedPrecisionValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNumericArrayScientificNotation() throws SQLException {
        List<String> scientificNotationValues = Arrays.asList(
            "1.23E-10",
            "1.23E+10",
            "1.23e-10",
            "1.23e+10",
            "1.23E0"
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(scientificNotationValues)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(scientificNotationValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNumericArrayUnsupportedSchemaType() throws SQLException {
        List<Long> numericValues = Arrays.asList(123L, 456L, 789L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(numericValues)
                .schemaType(Schema.Type.INT64)
                .build();

        // Should default to "numeric" type since it's not STRING
        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("numeric"), eq(numericValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithByteaArrayType() throws SQLException {
        List<String> byteaValues = Arrays.asList(
            "Hello World",
            "Test string",
            "123456789"
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(byteaValues)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        byte[][] expectedBytes = byteaValues.stream()
                .map(s -> s == null ? null : s.getBytes())
                .toArray(byte[][]::new);

        verify(mockConnection).createArrayOf(eq("bytea"), eq(expectedBytes));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithByteaArrayTypeEmptyString() throws SQLException {
        List<String> byteaValues = Arrays.asList("", "Test", "");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(byteaValues)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        Object[] expectedValues = new Object[]{
            new byte[]{},
            "Test".getBytes(),
            new byte[]{}
        };

        verify(mockConnection).createArrayOf(eq("bytea"), eq(expectedValues));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithByteaArrayTypeContainingNulls() throws SQLException {
        List<String> byteaValues = Arrays.asList(
            "Hello",
            null,
            "World"
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(byteaValues)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        Object[] expectedValues = new Object[]{
            "Hello".getBytes(),
            null,
            "World".getBytes()
        };

        verify(mockConnection).createArrayOf(eq("bytea"), eq(expectedValues));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyByteaArray() throws SQLException {
        List<String> emptyArray = new ArrayList<>();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(emptyArray)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        verify(mockConnection).createArrayOf(eq("bytea"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithLargeByteaArray() throws SQLException {
        List<String> largeArray = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String testData = "Test data " + i;
            largeArray.add(testData);
        }
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(largeArray)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        Object[] expectedValues = largeArray.stream()
                .map(s -> s.getBytes())
                .toArray();

        verify(mockConnection).createArrayOf(eq("bytea"), eq(expectedValues));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithByteaArrayTypeBinaryData() throws SQLException {
        byte[] binaryData1 = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
        byte[] binaryData2 = {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD, (byte) 0xFC};
        
        List<Object> byteaValues = Arrays.asList(
            binaryData1,
            binaryData2
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(byteaValues)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        Object[] expectedValues = new Object[]{binaryData1, binaryData2};

        verify(mockConnection).createArrayOf(eq("bytea"), eq(expectedValues));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithByteaArrayTypeSpecialCharacters() throws SQLException {
        String specialChars = "!@#$%^&*()_+-=[]{}|;':\",./<>?";
        String unicodeText = "Unicode: café, naïve, résumé, Москва";
        
        List<String> byteaValues = Arrays.asList(
            specialChars,
            unicodeText
        );
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(byteaValues)
                .build();

        TableSchema.Column byteaArrayColumn = new TableSchema.Column("test_column", "array(bytea)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, byteaArrayColumn);

        Object[] expectedValues = new Object[]{
            specialChars.getBytes(),
            unicodeText.getBytes()
        };

        verify(mockConnection).createArrayOf(eq("bytea"), eq(expectedValues));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNullValueThrowsException() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(null)
                .build();

        assertThrows(NullPointerException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);
        });
    }

    @Test
    void testConvertAndSetWithNonListValueThrowsException() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value("not a list")
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);
        });
    }

    @Test
    void testConvertAndSetWithSQLExceptionFromConnection() throws SQLException {
        List<Long> arrayValues = Arrays.asList(1L, 2L, 3L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();
        
        when(mockConnection.createArrayOf(any(String.class), any(Object[].class)))
                .thenThrow(new SQLException("Database connection error"));

        assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);
        });
    }

    @ParameterizedTest
    @CsvSource({
        "1, 'First parameter'",
        "2, 'Second parameter'",
        "10, 'Tenth parameter'",
        "100, 'Hundredth parameter'"
    })
    void testConvertAndSetWithDifferentParameterIndices(int paramIndex, String description) throws SQLException {
        List<Long> arrayValues = Arrays.asList(1L, 2L, 3L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, paramIndex, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(paramIndex, mockArray);
    }

} 