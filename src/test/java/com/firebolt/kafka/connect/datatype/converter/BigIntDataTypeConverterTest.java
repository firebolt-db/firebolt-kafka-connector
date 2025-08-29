package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
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

public class BigIntDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private BigIntDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new BigIntDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "bigint", 8, true);
    }

    @ParameterizedTest
    @CsvSource({
        "0",                    // Zero
        "42",                   // Positive value
        "-42",                  // Negative value
        "1",                    // Small positive
        "-1",                   // Small negative
        "1000000000",           // Large positive
        "-1000000000",          // Large negative
        "1234567890123456789",  // Very large positive
        "-1234567890123456789", // Very large negative
        "-9223372036854775808", // Long.MIN_VALUE
        "9223372036854775807"   // Long.MAX_VALUE
    })
    void testConvertAndSetWithValidLongValues(String longValueStr) throws SQLException {
        Long longValue = Long.parseLong(longValueStr);
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(longValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setLong(1, longValue);
    }

    @Test
    void testConvertAndSetWithNonLongValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(new Object())
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "42",
            "-42",
            "9223372036854775807",
            "-9223372036854775808"
    })
    void testConvertAndSetWithValidStringLongs(String input) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(input)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setLong(1, Long.parseLong(input));
    }

    @ParameterizedTest
    @CsvSource({
            "' '",
            "abc",
            "1.23",
            "9223372036854775808",
            "-9223372036854775809"
    })
    void testConvertAndSetWithInvalidStringLongsThrowsColumnConversionFailed(String input) {
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

        verify(mockStatement).setLong(1, (long) value);
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

        verify(mockStatement).setLong(1, (long) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "2147483647",
            "-2147483648",
            "123456",
            "-123456"
    })
    void testConvertAndSetWithIntegerValues(int value) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setLong(1, (long) value);
    }
}