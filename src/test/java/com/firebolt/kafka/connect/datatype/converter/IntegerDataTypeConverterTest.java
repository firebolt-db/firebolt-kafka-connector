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

public class IntegerDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private IntegerDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new IntegerDataTypeConverter();
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
    void testConvertAndSetWithValidIntegers(long longValue) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(longValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, (int) longValue);
    }

    @Test
    void testConvertAndSetWithValueBeyondIntegerMaxTruncatesValue() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(Long.MAX_VALUE)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, -1); // Long.MAX_VALUE.intValue() = -1
    }

    @Test
    void testConvertAndSetWithValueBeyondIntegerMinTruncatesValue() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(Long.MIN_VALUE)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setInt(1, 0); // Long.MIN_VALUE.intValue() = 0
    }

    @Test
    void testConvertAndSetWithNullValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(null)
                .build();

        assertThrows(NullPointerException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }

    @Test
    void testConvertAndSetWithNonLongValueThrowsException() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("not a number")
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }

} 