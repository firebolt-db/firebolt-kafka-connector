package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import org.apache.kafka.connect.data.Schema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class TimestampDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaTimestampDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaTimestampDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "timestamp", 93, true);
    }

    @ParameterizedTest
    @CsvSource({
        "0",                    // Epoch
        "1000",                 // 1 second in millis
        "1609459200000",        // 2021-01-01 00:00:00 UTC in millis
        "1234567890123",        // Random timestamp in millis
        "10000000000000",       // Threshold value (treated as millis)
        "10000000000001",       // Just above threshold (treated as micros)
        "1609459200000000",     // 2021-01-01 00:00:00 UTC in micros
        "-1000"                 // Negative value (before epoch)
    })
    void testConvertAndSetWithInt64SchemaType(long timestampValue) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampValue)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        Timestamp expectedTimestamp = TimestampUtil.asTimestamp(timestampValue);
        verify(mockStatement).setTimestamp(1, expectedTimestamp);
    }

    @ParameterizedTest
    @CsvSource({
        "'2021-01-01 00:00:00'",
        "'2023-12-25 23:59:59.999'",
        "'1970-01-01 00:00:00.0'",
        "'2000-02-29 12:30:45'",
        "'1999-12-31 23:59:59.123456'",
        "'2024-01-15T14:30:45'",
        "'2024-01-15T14:30:45.123456789'",
        "'2024-01-15T14:30:45.123456789Z'",
        "'2024-01-15 14:30:45.123456789Z'"
    })
    void testConvertAndSetWithStringSchemaType(String timestampString) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(timestampString)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, timestampString);
    }

    @Test
    void testConvertAndSetWithNullInt64Value() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(null)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setTimestamp(1, null);
    }

    @Test
    void testConvertAndSetWithNullStringValue() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(null)
                .schemaType(Schema.Type.STRING)
                .build();

        org.junit.jupiter.api.Assertions.assertThrows(ColumnConversionFailedException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }

    @Test
    void testConvertAndSetWithInt64SchemaTypeButNonLongValue() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value("not a long")
                .schemaType(Schema.Type.INT64)
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
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

    @Test
    void testConvertAndSetWithZeroTimestamp() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(0L)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        Timestamp expectedTimestamp = new Timestamp(0L);
        verify(mockStatement).setTimestamp(1, expectedTimestamp);
    }

    @Test
    void testConvertAndSetWithDateValue() throws SQLException {
        java.util.Date date = new java.util.Date(1700000000000L);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(date)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setTimestamp(1, new Timestamp(date.getTime()));
    }

    @ParameterizedTest
    @CsvSource({
        "'2024-01-15T14:30:45+01:00'",
        "'not-a-date'",
        "'2024-01-15T14:30:45.'"
    })
    void testConvertAndSetWithInvalidStringShouldThrow(String invalid) {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(invalid)
                .schemaType(Schema.Type.STRING)
                .build();

        assertThrows(ColumnConversionFailedException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
    }
}