package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
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

public class DoubleDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private DoubleDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new DoubleDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "double", 8, true);
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
        "3.14159265359",          // Pi
        "-3.14159265359",         // Negative Pi
        "1.23E-10",               // Very small positive
        "-1.23E-10",              // Very small negative
        "1.23E+10",               // Very large positive
        "-1.23E+10",              // Very large negative
        "Double.MAX_VALUE",       // Maximum double value
        "Double.MIN_VALUE",       // Minimum double value
        "Double.NEGATIVE_INFINITY", // Negative infinity
        "Double.POSITIVE_INFINITY"  // Positive infinity
    })
    void testConvertAndSetWithValidDoubleValues(String doubleValueStr) throws SQLException {
        Double doubleValue;
        
        switch (doubleValueStr) {
            case "Double.MAX_VALUE":
                doubleValue = Double.MAX_VALUE;
                break;
            case "Double.MIN_VALUE":
                doubleValue = Double.MIN_VALUE;
                break;
            case "Double.NEGATIVE_INFINITY":
                doubleValue = Double.NEGATIVE_INFINITY;
                break;
            case "Double.POSITIVE_INFINITY":
                doubleValue = Double.POSITIVE_INFINITY;
                break;
            default:
                doubleValue = Double.parseDouble(doubleValueStr);
        }
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(doubleValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, doubleValue);
    }

    @Test
    void testConvertAndSetWithNonDoubleValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("not a number")
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagation() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(123.45)
                .build();

        // Mock the statement to throw SQLException
        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setDouble(1, 123.45);

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }

}