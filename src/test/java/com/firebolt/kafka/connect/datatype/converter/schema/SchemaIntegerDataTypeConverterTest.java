package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class SchemaIntegerDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaIntegerDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaIntegerDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "integer", 4, true);
    }

    @ParameterizedTest
    @CsvSource({
        "-2147483648", // Integer.MIN_VALUE
        "2147483647",  // Integer.MAX_VALUE
        "0",           // Zero
        "42",          // Positive value
        "-42",         // Negative value
        "1",           // Small positive
        "-1",          // Small negative
        "100000",      // Large positive
        "-100000"      // Large negative
    })
    void testConvertAndSetWithValidIntegers(int intValue) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(intValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, (int) intValue);
    }

    @Test
    void testConvertAndSetWithValueBeyondIntegerMaxThrowsConversionFailed() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(Long.MAX_VALUE)
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithValueBeyondIntegerMinThrowsConversionFailed() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(Long.MIN_VALUE)
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithNullValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(null)
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithNonConvertibleTypeThrowsColumnConversionFailed() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(Set.of(1, 2, 3))
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @ParameterizedTest
    @CsvSource({
            "0,0",
            "42,42",
            "-7,-7",
            "2147483647,2147483647",
            "-2147483648,-2147483648"
    })
    void testConvertAndSetWithValidIntegerStrings(String input, int expected) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(input)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, expected);
    }

    @ParameterizedTest
    @CsvSource({
            "' '",
            "abc",
            "1.23",
            "+-1",
            "2147483648",   // overflow
            "-2147483649"   // underflow
    })
    void testConvertAndSetWithInvalidIntegerStringsThrowsColumnConversionFailed(String input) {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(input)
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "127",
            "-128",
            "42",
            "-42"
    })
    void testConvertAndSetWithByteValues(byte value) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, (int) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "32767",
            "-32768",
            "1024",
            "-1024"
    })
    void testConvertAndSetWithShortValues(short value) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, (int) value);
    }

} 