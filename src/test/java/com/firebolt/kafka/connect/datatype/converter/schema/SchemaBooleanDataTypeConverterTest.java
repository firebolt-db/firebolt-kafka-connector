package com.firebolt.kafka.connect.datatype.converter.schema;

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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SchemaBooleanDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaBooleanDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaBooleanDataTypeConverter();
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

    @Test
    void testConvertAndSetWithSQLExceptionPropagationForString() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("true")
                .build();

        // Mock the statement to throw SQLException for setString
        org.mockito.Mockito.doThrow(new SQLException("Database error for string"))
                .when(mockStatement).setString(1, "true");

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error for string", exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
            "true",
            "false",
            "TRUE",
            "FALSE",
            "t",
            "f",
            "T",
            "F",
            "1",
            "0"
    })
    void testConvertAndSetWithValidBooleanStrings(String stringValue) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(stringValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, stringValue);
    }

    @Test
    void willThrowExceptionWhenCannotDeserializeColumnAsBoolean() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("not_a_boolean")
                .build();

        ColumnConversionFailedException exception = assertThrows(ColumnConversionFailedException.class, () -> converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
        verify(mockStatement, never()).setBoolean(anyInt(), anyBoolean());
        verify(mockStatement, never()).setString(anyInt(), anyString());
        assertEquals("boolean", exception.getColumnType());
        assertEquals("test_column", exception.getColumnName());
        assertEquals("Cannot convert kafka message attribute to a boolean due to incompatible type: java.lang.String", exception.getMessage());
    }
}