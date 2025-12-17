package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaTimestampBinaryColumnDataTypeConverterTest {

    private final SchemaTimestampBinaryColumnDataTypeConverter converter = new SchemaTimestampBinaryColumnDataTypeConverter();
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
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(iso)
                .build(), tsColumn);
        assertEquals(expectedMicros, result.longValue());
    }

    @Test
    void convertsEpochMillisToMicros() {
        long millis = 1_700_000_000_123L;
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(millis)
                .build(), tsColumn);
        assertEquals(millis * 1_000L, result.longValue());
    }

    @Test
    void keepsEpochMicrosAsIs() {
        long micros = 1_700_000_000_000_987L;
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(micros)
                .build(), tsColumn);
        assertEquals(micros, result.longValue());
    }

    @Test
    void convertsJavaUtilDateToMicros() {
        Date date = Date.from(Instant.ofEpochMilli(1_700_000_000_123L));
        long expectedMicros = date.toInstant().getEpochSecond() * 1_000_000L + (date.toInstant().getNano() / 1_000L);
        Long result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(date)
                .build(), tsColumn);
        assertEquals(expectedMicros, result.longValue());
    }

    @Test
    void invalidStringThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                        .schemaType(Schema.Type.STRING)
                        .value("not-a-timestamp")
                        .build(), tsColumn));
        assertEquals("ts", ex.getColumnName());
        assertEquals("timestamp", ex.getColumnType());
    }

    private static long toMicros(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
    }
}
