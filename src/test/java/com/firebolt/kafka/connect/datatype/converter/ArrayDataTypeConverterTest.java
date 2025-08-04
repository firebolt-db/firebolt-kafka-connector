package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ArrayDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    @Mock
    private Connection mockConnection;

    @Mock
    private Array mockArray;

    private ArrayDataTypeConverter converter;
    private TableSchema.Column integerArrayColumn;
    private TableSchema.Column textArrayColumn;
    private TableSchema.Column timestampArrayColumn;
    private TableSchema.Column numericArrayColumn;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        converter = new ArrayDataTypeConverter();
        integerArrayColumn = new TableSchema.Column("test_column", "array(integer)", 2003, true);
        textArrayColumn = new TableSchema.Column("test_column", "array(text)", 2003, true);
        timestampArrayColumn = new TableSchema.Column("test_column", "array(timestamp)", 2003, true);
        numericArrayColumn = new TableSchema.Column("test_column", "array(numeric)", 2003, true);
        
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithEmptyArray() throws SQLException {
        List<Long> emptyArray = new ArrayList<>();
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(emptyArray)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(emptyArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithArrayContainingZerosAndNulls() throws SQLException {
        List<Long> arrayValues = Arrays.asList(0L, 0L, 0L, null, null);
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(largeArray)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);

        verify(mockConnection).createArrayOf(eq("integer"), eq(largeArray.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithTextArrayType() throws SQLException {
        List<Long> arrayValues = Arrays.asList(1L, 2L, 3L);
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, textArrayColumn);

        verify(mockConnection).createArrayOf(eq("string"), eq(arrayValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithBigIntArrayType() throws SQLException {
        List<Long> bigIntValues = Arrays.asList(1L, 2L, 3L);
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(bigIntValues)
                .build();

        TableSchema.Column bigIntArrayColumn = new TableSchema.Column("test_column", "array(bigint)", 2003, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, bigIntArrayColumn);

        verify(mockConnection).createArrayOf(eq("bigint"), eq(bigIntValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @ParameterizedTest
    @CsvSource({
        "array(real)",
        "array(boolean)",
        "array(date)",
        "unsupported_array_type"
    })
    void testConvertAndSetWithUnsupportedArrayTypesDefaultsToString(String dataType) throws SQLException {
        TableSchema.Column unsupportedColumn = new TableSchema.Column("test_column", dataType, 2003, true);
        List<Long> arrayValues = Arrays.asList(1L, 2L, 3L);
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(numericValues)
                .schemaType(Schema.Type.INT64)
                .build();

        // Should default to "numeric" type since it's not STRING
        converter.convertAndSet(mockStatement, 1, kafkaValue, numericArrayColumn);

        verify(mockConnection).createArrayOf(eq("numeric"), eq(numericValues.toArray()));
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void testConvertAndSetWithNullValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(null)
                .build();

        assertThrows(NullPointerException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);
        });
    }

    @Test
    void testConvertAndSetWithNonListValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("not a list")
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);
        });
    }

    @Test
    void testConvertAndSetWithSQLExceptionFromConnection() throws SQLException {
        List<Long> arrayValues = Arrays.asList(1L, 2L, 3L);
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(arrayValues)
                .build();
        
        when(mockConnection.createArrayOf(any(String.class), any(Object[].class)))
                .thenThrow(new SQLException("Database connection error"));

        assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, integerArrayColumn);
        });
    }
} 