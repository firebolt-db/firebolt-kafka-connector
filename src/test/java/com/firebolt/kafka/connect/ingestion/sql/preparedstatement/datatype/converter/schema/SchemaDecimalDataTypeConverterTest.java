package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class SchemaDecimalDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaDecimalDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaDecimalDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "decimal", 2, true);
    }

    @Test
    void testConvertAndSetWithBigDecimal() throws SQLException {
        java.math.BigDecimal bd = new java.math.BigDecimal("123.456");
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(bd)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, bd.toString());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
            "0",
            "1",
            "-1",
            "42",
            "-42",
            "32767",
            "-32768"
    })
    void testConvertAndSetWithIntegralNumbers(short value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, java.math.BigDecimal.valueOf((long) value).toString());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
            "0.0",
            "1.5",
            "-1.5",
            "42.25",
            "-42.25"
    })
    void testConvertAndSetWithFloatingNumbers(double value) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, java.math.BigDecimal.valueOf(value).toString());
    }

    @ParameterizedTest
    @CsvSource({
        "'123.45'",
        "'0.00'",
        "'-123.45'",
        "'1.23E-10'",  // scientific notation
        "'999999.99'",
        "'0.01'",
        "'1000000.00'",
        "'123456789.123456789'",
        "'12345678901234567890123456789.123456789'",
        "'0'",
        "'-0.001'"
    })
    void testConvertAndSetWithStringSchemaType(String decimalString) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(decimalString)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, decimalString);
    }

    @Test
    void testConvertAndSetWithStringSchemaTypeButNonStringValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(12345L)
                .schemaType(Schema.Type.STRING)
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"abc", "", " ", "+-1", "1,23"})
    void testConvertAndSetWithInvalidDecimalStringThrowsColumnConversionFailed(String input) {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(input)
                .schemaType(Schema.Type.STRING)
                .build();

        org.junit.jupiter.api.Assertions.assertThrows(
                ColumnConversionFailedException.class,
                () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn)
        );
    }

    @Test
    void testConvertAndSetWithUnsupportedTypeThrowsColumnConversionFailed() {
        Object unsupported = new Object();
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(unsupported)
                .build();

        org.junit.jupiter.api.Assertions.assertThrows(
                ColumnConversionFailedException.class,
                () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn)
        );
    }

} 