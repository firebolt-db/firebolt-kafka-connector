package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessTimestampBinaryColumnDataTypeConverterTest {

    private final SchemalessTimestampBinaryColumnDataTypeConverter converter = new SchemalessTimestampBinaryColumnDataTypeConverter();
    private final TableSchema.Column tsColumn = new TableSchema.Column("ts", "timestamp", Types.TIMESTAMP, false);

    @ParameterizedTest
    @CsvSource({
            "2025-01-02T03:04:05",
            "2025-01-02 03:04:05",
            "2025-01-02T03:04:05.123456",
            "2025-01-02 03:04:05.123456",
            "2025-01-02T03:04:05Z",
            "2025-01-02 03:04:05Z"
    })
    void parsesIsoStringsToMicros(String iso) {
        long expectedMicros = toMicros(FireboltTimestampConverter.parseIsoLocalDateTime(iso));
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(iso), tsColumn);
        assertEquals(expectedMicros, result.longValue());
    }

    @Test
    void convertsEpochMillisToMicros() {
        long millis = 1_700_000_000_000L;
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(millis), tsColumn);
        assertEquals(millis * 1_000L, result.longValue());
    }

    @Test
    void keepsEpochMicrosAsIs() {
        long micros = 1_700_000_000_000_000L;
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(micros), tsColumn);
        assertEquals(micros, result.longValue());
    }

    @Test
    void invalidStringThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("not-a-timestamp"), tsColumn));
        assertEquals("ts", ex.getColumnName());
        assertEquals("timestamp", ex.getColumnType());
    }

    private static long toMicros(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
    }
}


