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

public class BooleanDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private BooleanDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new BooleanDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "boolean", 16, true);
    }

    @ParameterizedTest
    @CsvSource({
            "true",
            "false"
    })
    void testConvertAndSetWithValidBooleans(boolean booleanValue) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(booleanValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBoolean(1, booleanValue);
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagation() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(true)
                .build();

        // Mock the statement to throw SQLException
        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setBoolean(1, true);

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }
}