package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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

public class DateDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private DateDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new DateDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "date", 91, true);
    }

    // Tests for INT32 schema type (Days since epoch - Kafka Connect Date logical type)
    
    @ParameterizedTest
    @CsvSource({
        "0, 1970-01-01",        // Unix epoch (day 0)
        "1, 1970-01-02",        // Day 1 after epoch
        "365, 1971-01-01",      // One year after epoch
        "19737, 2024-01-15",    // The example from DateSerializerTest
        "18628, 2021-01-01",    // New Year 2021
        "19358, 2023-01-01",    // New Year 2023
        "20089, 2025-01-01",    // Future date 2025
        "10957, 2000-01-01",    // Y2K
        "18262, 2020-01-01",    // Leap year 2020
        "15003, 2011-01-29",    // Random date
        "17532, 2018-01-01",    // Another date
        "11323, 2001-01-01",    // 2001
        "-1, 1969-12-31",       // One day before epoch
        "-365, 1969-01-01"      // One year before epoch
    })
    void testConvertAndSetWithInt32SchemaType(String daysStr, String expectedDateStr) throws SQLException {
        Integer daysValue = Integer.parseInt(daysStr);
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(daysValue)
                .schemaType(Schema.Type.INT32)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        // Verify that setDate is called with the correct Date object
        Date expectedDate = Date.valueOf(expectedDateStr);
        verify(mockStatement).setDate(1, expectedDate);
    }

    // Tests for STRING schema type

    @ParameterizedTest
    @CsvSource({
        "'2023-01-01'",
        "'2023-12-31'",
        "'2000-02-29'",         // Leap year
        "'1900-01-01'",         // Historical date
        "'2025-06-15'",         // Future date
        "'2023-02-14'",         // Valentine's Day
        "'2023-07-04'",         // Independence Day
        "'2023-10-31'",         // Halloween
        "'2023-12-25'",         // Christmas
        "'1970-01-01'",         // Unix epoch
        "'9999-12-31'",         // Max date
        "'0001-01-01'",         // Min date
        "'2020-02-29'",         // Leap year Feb 29
        "'2021-02-28'",         // Non-leap year Feb 28
        "'2023-06-30'"          // End of June
    })
    void testConvertAndSetWithStringSchemaType(String dateString) throws SQLException {
        // Remove quotes from CSV source parameter
        String actualDateString = dateString.substring(1, dateString.length() - 1);
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(actualDateString)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, actualDateString);
    }

    @Test
    void testConvertAndSetWithInt32SQLExceptionPropagation() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(19358) // 2023-01-01 as days since epoch
                .schemaType(Schema.Type.INT32)
                .build();

        Date expectedDate = Date.valueOf("2023-01-01");
        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setDate(1, expectedDate);

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }
}