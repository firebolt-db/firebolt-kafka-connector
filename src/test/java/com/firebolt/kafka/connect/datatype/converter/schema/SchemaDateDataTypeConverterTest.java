package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class SchemaDateDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaDateDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaDateDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "date", 91, true);
    }

    @Test
    void testConvertAndSetWithUtilDate() throws SQLException {
        Date sqlDate = Date.valueOf("2023-01-01");
        java.util.Date utilDate = new java.util.Date(sqlDate.getTime());

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(utilDate)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setDate(1, Date.valueOf("2023-01-01"));
    }

    @ParameterizedTest
    @CsvSource({
        "2023-01-01",
        "2023-12-31",
        "2000-02-29",
        "1900-01-01",
        "2025-06-15",
        "2023-02-14",
        "2023-07-04",
        "2023-10-31",
        "2023-12-25",
        "1970-01-01",
        "9999-12-31",
        "0001-01-01",
        "2020-02-29",
        "2021-02-28",
        "2023-06-30"
    })
    void testConvertAndSetWithValidIsoDateString(String dateString) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(dateString)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, dateString);
    }

    @ParameterizedTest
    @CsvSource({
        "abc",
        "2024-1-02",
        "2024-01-2",
        "2024-02-30",
        "''",
        "2024/01/01",
        "01-01-2024",
        "2024-13-01",
        "2024-00-10"
    })
    void testThrowsOnInvalidIsoDateString(String invalid) {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(invalid)
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testThrowsOnNullValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(null)
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testSQLExceptionPropagationForSetDate() throws SQLException {
        Date sqlDate = Date.valueOf("2023-01-01");
        java.util.Date utilDate = new java.util.Date(sqlDate.getTime());

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(utilDate)
                .build();

        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setDate(eq(1), any(Date.class));

        SQLException exception = assertThrows(SQLException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));

        assertEquals("Database error", exception.getMessage());
    }

    @Test
    void testSQLExceptionPropagationForSetString() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value("2023-01-01")
                .build();

        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setString(1, "2023-01-01");

        SQLException exception = assertThrows(SQLException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));

        assertEquals("Database error", exception.getMessage());
    }
}