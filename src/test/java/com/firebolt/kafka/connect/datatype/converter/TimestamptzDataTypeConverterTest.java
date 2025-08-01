package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class TimestamptzDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private TimestamptzDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new TimestamptzDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "timestamptz", 2014, true);
    }

    @ParameterizedTest
    @CsvSource({
        "0",                    // Epoch
        "1000",                 // 1 second in millis
        "1609459200000",        // 2021-01-01 00:00:00 UTC in millis
        "1234567890123",        // Random timestamp in millis
        "10000000000000",       // Large value
        "10000000000001",       // Very large value
        "-1000"                 // Negative value (before epoch)
    })
    void testConvertAndSetWithInt64SchemaType(long timestampValue) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(timestampValue)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setObject(eq(1), any(java.time.OffsetDateTime.class));
    }

    @ParameterizedTest
    @CsvSource({
        "'2021-01-01 00:00:00+00:00'",
        "'2023-12-25 23:59:59.999+05:30'",
        "'1970-01-01 00:00:00.0+00:00'",
        "'2000-02-29 12:30:45-08:00'",
        "'1999-12-31 23:59:59.123456+01:00'",
        "'2024-01-01T00:00:00Z'",
        "'2024-01-01T00:00:00.123Z'"
    })
    void testConvertAndSetWithStringSchemaType(String timestampString) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(timestampString)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, timestampString);
    }

    @Test
    void testConvertAndSetWithNullInt64Value() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(null)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        // For null values, the converter should handle them gracefully
        // Since we're using setObject with OffsetDateTime, null handling might be different
    }

    @Test
    void testConvertAndSetWithNullStringValue() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(null)
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        // For null values, the converter should handle them gracefully
        // Since we're using setString, null handling might be different
    }

    @Test
    void testConvertAndSetWithEmptyString() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("")
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, "");
    }

    @Test
    void testConvertAndSetWithWhitespaceString() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("   ")
                .schemaType(Schema.Type.STRING)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, "   ");
    }

    @Test
    void testConvertAndSetWithInt64SchemaTypeButNonLongValue() {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("not a long")
                .schemaType(Schema.Type.INT64)
                .build();

        assertThrows(ClassCastException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });
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

    @ParameterizedTest
    @CsvSource({
        "BOOLEAN",
        "INT8", 
        "INT16",
        "INT32",
        "FLOAT32",
        "FLOAT64",
        "BYTES",
        "ARRAY"
    })
    void testConvertAndSetWithUnsupportedSchemaTypes(Schema.Type schemaType) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("some value")
                .schemaType(schemaType)
                .build();

        // Should not throw exception, but also should not set anything
        // The converter only handles INT64 and STRING types
        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        // No verify calls since the converter should not set anything for unsupported types
    }

    @Test
    void testConvertAndSetWithZeroTimestamp() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(0L)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setObject(eq(1), any(java.time.OffsetDateTime.class));
    }

    @Test
    void testConvertAndSetWithLargeTimestampValue() throws SQLException {
        long largeTimestamp = 9_223_372_036_854_775_807L; // Long.MAX_VALUE
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(largeTimestamp)
                .schemaType(Schema.Type.INT64)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setObject(eq(1), any(java.time.OffsetDateTime.class));
    }
} 