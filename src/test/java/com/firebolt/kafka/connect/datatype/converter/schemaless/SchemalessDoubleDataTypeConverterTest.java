package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class SchemalessDoubleDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessDoubleDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessDoubleDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "double", 8, true);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "1",
            "-1",
            "42",
            "-42",
            "127",
            "-128"
    })
    void testConvertAndSetWithByteValues(byte value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, (double) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "1",
            "-1",
            "42",
            "-42",
            "32767",
            "-32768"
    })
    void testConvertAndSetWithShortValues(short value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, (double) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "1",
            "-1",
            "42",
            "-42",
            "100000",
            "-100000"
    })
    void testConvertAndSetWithIntValues(int value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, (double) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "1",
            "-1",
            "42",
            "-42",
            "10000000000",
            "-10000000000"
    })
    void testConvertAndSetWithLongValues(long value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, (double) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0.0",
            "1.5",
            "-1.5",
            "42.25",
            "-42.25"
    })
    void testConvertAndSetWithFloatValues(float value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, (double) value);
    }

    @ParameterizedTest
    @CsvSource({
            "0.0",
            "42.5",
            "-42.5",
            "1.23E-10",
            "1.23E+10"
    })
    void testConvertAndSetWithDoubleValues(double value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, value);
    }

    @ParameterizedTest
    @CsvSource({
            "0",
            "42.5",
            "-42.5",
            "1e3",
            "-1e-3",
            "  12.34  "
    })
    void testConvertAndSetWithValidStringValues(String input) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(input)
                .build();

        double expected = Double.parseDouble(input.trim());

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDouble(1, expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "", " ", "+-1", "1,23"})
    void testConvertAndSetWithInvalidStringValuesThrowsColumnConversionFailed(String input) {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(input)
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagation() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123.45)
                .build();

        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setDouble(1, 123.45);

        SQLException exception = assertThrows(SQLException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));

        assertEquals("Database error", exception.getMessage());
    }
}