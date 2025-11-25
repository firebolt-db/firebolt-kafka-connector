package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessArrayColumnTimestampDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessTimestampColumnDataTypeConverter tsConverter = new SchemalessTimestampColumnDataTypeConverter();
    private final TableSchema.Column arrayTsColumn = new TableSchema.Column("timestamps", "array(timestamp)", Types.ARRAY, false);

    @Test
    void convertsMixedElementsToMicros() {
        arrayConverter.setTimestampConverter(tsConverter);
        String iso = "2025-01-02T00:00:00";
        long expectedFromIso = toMicros(FireboltTimestampConverter.parseIsoLocalDateTime(iso));
        long millis = 1_700_000_000_000L;
        long micros = 1_700_000_000_000_000L;

        List<Object> input = Arrays.asList(iso, millis, null, micros);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayTsColumn);
        assertEquals(Arrays.asList(expectedFromIso, millis * 1_000L, null, micros), result);
    }

    @Test
    void invalidElementCausesFailure() {
        arrayConverter.setTimestampConverter(tsConverter);
        List<Object> input = Arrays.asList("not-iso");
        assertThrows(ColumnConversionFailedException.class,
                () -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayTsColumn));
    }

    private static long toMicros(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
    }
}


