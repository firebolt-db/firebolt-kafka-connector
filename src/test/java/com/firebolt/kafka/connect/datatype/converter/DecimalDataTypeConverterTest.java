package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
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

public class DecimalDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private DecimalDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new DecimalDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "decimal", 2, true);
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
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(decimalString)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, decimalString);
    }

    @Test
    void testConvertAndSetWithStringSchemaTypeButNonStringValue() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(12345L)
                .schemaType(Schema.Type.STRING)
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }

} 