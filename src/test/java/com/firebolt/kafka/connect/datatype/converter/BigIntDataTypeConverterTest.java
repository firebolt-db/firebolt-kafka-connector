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
                .value("not a number")
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }
}