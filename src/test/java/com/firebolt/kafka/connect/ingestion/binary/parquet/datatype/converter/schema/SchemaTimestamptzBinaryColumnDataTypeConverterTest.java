package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaTimestamptzBinaryColumnDataTypeConverterTest {

    private final SchemaTimestamptzBinaryColumnDataTypeConverter converter = new SchemaTimestamptzBinaryColumnDataTypeConverter();
    private final TableSchema.Column col = new TableSchema.Column("tz", "timestamptz", Types.TIMESTAMP_WITH_TIMEZONE, false);

    @ParameterizedTest
    @ValueSource(strings = {
            "2025-01-02T03:04:05Z",
            "2025-01-02 03:04:05Z",
            "2025-01-02T03:04:05+00:00",
            "2025-01-02 05:04:05+02:00",
            "2025-01-02T03:04:05.123456Z"
    })
    void parsesIsoOffsetStringsToMicros(String input) {
        OffsetDateTime odt = FireboltTimestamptzConverter.parseTimestamptz(input).withOffsetSameInstant(ZoneOffset.UTC);
        Instant instant = odt.toInstant();
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        long expectedMicros = seconds * 1_000_000L + (nanos / 1_000);
        if ((nanos % 1_000) >= 500) {
            expectedMicros += 1;
        }

        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(input)
                .build(), col);
        assertEquals(expectedMicros, result.longValue());
    }

    @Test
    void convertsEpochMillisToMicros() {
        long millis = 1_700_000_000_123L;
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(millis)
                .build(), col);
        assertEquals(millis * 1_000L, result.longValue());
    }

    @Test
    void keepsEpochMicrosAsIs() {
        long micros = 1_700_000_000_000_987L;
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(micros)
                .build(), col);
        assertEquals(micros, result.longValue());
    }

    @Test
    void convertsJavaUtilDateToMicros() {
        Date date = Date.from(Instant.ofEpochMilli(1_700_000_000_123L));
        long expectedMicros = date.toInstant().getEpochSecond() * 1_000_000L + (date.toInstant().getNano() / 1_000L);
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(date)
                .build(), col);
        assertEquals(expectedMicros, result.longValue());
    }

    @Test
    void invalidStringThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                        .schemaType(Schema.Type.STRING)
                        .value("not-a-timestamptz")
                        .build(), col));
        assertEquals("tz", ex.getColumnName());
        assertEquals("timestamptz", ex.getColumnType());
    }
}


