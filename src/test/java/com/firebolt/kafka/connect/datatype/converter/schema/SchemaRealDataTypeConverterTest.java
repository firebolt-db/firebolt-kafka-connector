package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class SchemaRealDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaRealDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaRealDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "real", 7, true);
    }

    @ParameterizedTest
    @CsvSource({
        "0.0",                    // Zero
        "42.5",                   // Positive value
        "-42.5",                  // Negative value
        "1.0",                    // Small positive
        "-1.0",                   // Small negative
        "1000000.0",              // Large positive
        "-1000000.0",             // Large negative
        "3.14159",                // Pi
        "-3.14159",               // Negative Pi
        "1.23E-10",               // Very small positive
        "-1.23E-10",              // Very small negative
        "1.23E+10",               // Very large positive
        "-1.23E+10",              // Very large negative
        "Float.MAX_VALUE",        // Maximum float value
        "Float.MIN_VALUE",        // Minimum float value
        "Float.NEGATIVE_INFINITY", // Negative infinity
        "Float.POSITIVE_INFINITY"  // Positive infinity
    })
    void testConvertAndSetWithValidFloatValues(String floatValueStr) throws SQLException {
        Float floatValue;
        
        switch (floatValueStr) {
            case "Float.MAX_VALUE":
                floatValue = Float.MAX_VALUE;
                break;
            case "Float.MIN_VALUE":
                floatValue = Float.MIN_VALUE;
                break;
            case "Float.NEGATIVE_INFINITY":
                floatValue = Float.NEGATIVE_INFINITY;
                break;
            case "Float.POSITIVE_INFINITY":
                floatValue = Float.POSITIVE_INFINITY;
                break;
            default:
                floatValue = Float.parseFloat(floatValueStr);
        }
        
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(floatValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, String.valueOf(floatValue));
    }

    @Test
    void testConvertAndSetWithNullValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(null)
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithNonFloatValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value("not a number")
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithIntegerValue() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(42) // Integer instead of Float
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setFloat(1, 42);
    }

    @Test
    void testConvertAndSetWithShortValue() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Short.valueOf("12"))
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setFloat(1, 12);
    }

    @Test
    void testConvertAndSetWithByteValue() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(Byte.valueOf("34"))
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setFloat(1, 34);
    }

    @Test
    void testConvertAndSetWithDoubleValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(42.5) // Double instead of Float
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithBooleanValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(true) // Boolean instead of Float
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithDifferentParameterIndex() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45f)
                .build();

        converter.convertAndSet(mockStatement, 5, kafkaValue, testColumn);

        verify(mockStatement).setString(5, "123.45");
    }

    @Test
    void testConvertAndSetWithNegativeParameterIndex() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45f)
                .build();

        converter.convertAndSet(mockStatement, -1, kafkaValue, testColumn);

        verify(mockStatement).setString(-1, "123.45");
    }

    @Test
    void testConvertAndSetWithLargeParameterIndex() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45f)
                .build();

        converter.convertAndSet(mockStatement, 1000, kafkaValue, testColumn);

        verify(mockStatement).setString(1000, "123.45");
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagation() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45f)
                .build();

        // Mock the statement to throw SQLException
        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setString(1, "123.45");

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }

    @Test
    void testConvertAndSetWithDifferentColumnTypes() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45f)
                .build();

        // Test with different column types - should still work as the converter doesn't use the column type
        TableSchema.Column intColumn = new TableSchema.Column("int_column", "integer", 4, true);
        TableSchema.Column realColumn = new TableSchema.Column("real_column", "real", 7, true);
        TableSchema.Column textColumn = new TableSchema.Column("text_column", "text", 0, true);

        converter.convertAndSet(mockStatement, 1, kafkaValue, intColumn);
        verify(mockStatement).setString(1, "123.45");

        converter.convertAndSet(mockStatement, 2, kafkaValue, realColumn);
        verify(mockStatement).setString(2, "123.45");

        converter.convertAndSet(mockStatement, 3, kafkaValue, textColumn);
        verify(mockStatement).setString(3, "123.45");
    }

    @Test
    void testConvertAndSetWithNullableColumn() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45f)
                .build();

        TableSchema.Column nullableColumn = new TableSchema.Column("nullable_column", "real", 7, false);

        converter.convertAndSet(mockStatement, 1, kafkaValue, nullableColumn);

        verify(mockStatement).setString(1, "123.45");
    }
} 