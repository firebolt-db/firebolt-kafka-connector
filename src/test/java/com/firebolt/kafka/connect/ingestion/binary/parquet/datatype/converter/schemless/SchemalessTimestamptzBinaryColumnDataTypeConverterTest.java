package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessTimestamptzBinaryColumnDataTypeConverterTest {

    private final SchemalessTimestamptzBinaryColumnDataTypeConverter converter = new SchemalessTimestamptzBinaryColumnDataTypeConverter();
    private final TableSchema.Column col = new TableSchema.Column("tsz", "timestamptz", Types.TIMESTAMP_WITH_TIMEZONE, false);

    @Test
    void doesNotRoundWhenSubMicroNanosBelowThreshold() {
        String iso = "2025-01-02T03:04:05.123456123Z"; // remainder 123 < 500
        Long micros = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(iso), col);

        OffsetDateTime odt = OffsetDateTime.parse("2025-01-02T03:04:05.123456123Z");
        Instant instant = odt.withOffsetSameInstant(ZoneOffset.UTC).toInstant();
        long expected = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000);
        // no +1 expected
        assertEquals(expected, micros.longValue());
    }

    @Test
    void roundsUpWhenSubMicroNanosAtOrAboveThreshold() {
        String iso = "2025-01-02T03:04:05.123456789Z"; // remainder 789 >= 500
        Long micros = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(iso), col);

        OffsetDateTime odt = OffsetDateTime.parse("2025-01-02T03:04:05.123456789Z");
        Instant instant = odt.withOffsetSameInstant(ZoneOffset.UTC).toInstant();
        long base = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000);
        long expected = base + 1;
        assertEquals(expected, micros.longValue());
    }

    @Test
    void roundsUpWhenSubMicroNanosExactly500() {
        String iso = "2025-01-02T03:04:05.123456500Z"; // remainder 500 => round up
        Long micros = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(iso), col);

        OffsetDateTime odt = OffsetDateTime.parse("2025-01-02T03:04:05.123456500Z");
        Instant instant = odt.withOffsetSameInstant(ZoneOffset.UTC).toInstant();
        long base = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000);
        long expected = base + 1;
        assertEquals(expected, micros.longValue());
    }

    @Test
    void convertsMillisNumberToMicros() {
        long millis = 1_700_000_000_000L;
        Long micros = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(millis), col);
        assertEquals(millis * 1_000L, micros.longValue());
    }

    @Test
    void passesThroughMicrosNumber() {
        long microValue = 1_700_000_000_000_123L;
        Long micros = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(microValue), col);
        assertEquals(microValue, micros.longValue());
    }

    @Test
    void supportsOffsetFormats() {
        String iso = "2025-01-02 03:04:05.123456+02:00";
        Long micros = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(iso), col);
        OffsetDateTime odt = OffsetDateTime.parse("2025-01-02T03:04:05.123456+02:00");
        Instant instant = odt.withOffsetSameInstant(ZoneOffset.UTC).toInstant();
        long expected = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000);
        assertEquals(expected, micros.longValue());
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("not-a-timestamptz"), col));
    }
}


