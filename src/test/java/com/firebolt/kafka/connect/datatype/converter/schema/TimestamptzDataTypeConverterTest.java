package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TimestamptzDataTypeConverterTest {

    private SchemaTimestamptzDataTypeConverter converter;
    private PreparedStatement statement;
    private TableSchema.Column column;

    @BeforeEach
    void setUp() {
        converter = new SchemaTimestamptzDataTypeConverter();
        statement = mock(PreparedStatement.class);
        column = new TableSchema.Column("col", "timestamptz", 1002, true);
    }

    @Test
    void convertsInt64MillisToOffsetDateTime() throws Exception {
        long millis = 1_700_000_000_000L;
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(millis)
                .build();

        converter.convertAndSet(statement, 1, value, column);

        OffsetDateTime expected = Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC);
        verify(statement).setObject(eq(1), eq(expected));
    }

    @Test
    void convertsInt64MicrosToOffsetDateTime() throws Exception {
        long micros = 1_700_000_000_123_456L;
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(micros)
                .build();

        converter.convertAndSet(statement, 1, value, column);

        OffsetDateTime expected = TimestampUtil.asOffsetDateTime(micros);
        verify(statement).setObject(eq(1), eq(expected));
    }

    @Test
    void convertsNullInt64ToNull() throws Exception {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(null)
                .build();

        converter.convertAndSet(statement, 1, value, column);

        verify(statement).setObject(eq(1), eq(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "2024-01-15T14:30:45Z",
            "2024-01-15 14:30:45.123456Z",
            "2024-01-15 12:30:45+00",
            "2024-01-15T12:30:45+00:00",
            "2024-01-15 14:30:45.12+02:00"
    })
    void acceptsValidStringTimestamptz(String input) throws Exception {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(input)
                .build();

        converter.convertAndSet(statement, 1, value, column);

        verify(statement).setString(eq(1), eq(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-a-date",
            "2024-01-15T14:30:45",           // missing zone
            "2024-02-30 00:00:00Z",          // invalid date
            "2024-01-15T14:30:45+2"          // invalid offset
    })
    void rejectsInvalidStringTimestamptz(String input) throws Exception {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(input)
                .build();

        assertThrows(ColumnConversionFailedException.class,
                () -> converter.convertAndSet(statement, 1, value, column));
    }

    @Test
    void rejectsInvalidStringThatIsNotATimestamptz() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value("not-a-timestamp")
                .build();

        assertThrows(ColumnConversionFailedException.class,
                () -> converter.convertAndSet(statement, 1, value, column));
    }

    @Test
    void rejectsUnsupportedSchemaType() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT32)
                .value(123)
                .build();

        assertThrows(ColumnConversionFailedException.class,
                () -> converter.convertAndSet(statement, 1, value, column));
    }
}