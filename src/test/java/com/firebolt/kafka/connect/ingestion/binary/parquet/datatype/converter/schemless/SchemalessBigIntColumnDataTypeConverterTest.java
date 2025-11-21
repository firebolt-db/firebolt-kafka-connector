package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessBigIntColumnDataTypeConverterTest {

    private final SchemalessBigIntColumnDataTypeConverter converter = new SchemalessBigIntColumnDataTypeConverter();
    private final TableSchema.Column bigintColumn = new TableSchema.Column("count64", "bigint", Types.BIGINT, false);

    @ParameterizedTest
    @CsvSource({
            "BYTE, 42",
            "SHORT, 42",
            "INT, 42",
            "LONG, 42",
            "STRING_NUMERIC, 42",
            "STRING_NUMERIC_WITH_SPACES, 42"
    })
    void convertsSupportedNumericRepresentations(String kind, long expected) {
        Object value = testValue(kind);
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), bigintColumn);
        assertEquals(expected, result.longValue());
    }

    @Test
    void stringNotNumericThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("abc"), bigintColumn));
        assertEquals("count64", ex.getColumnName());
        assertEquals("bigint", ex.getColumnType());
    }

    private static Object testValue(String kind) {
        switch (kind) {
            case "BYTE":
                return Byte.valueOf((byte) 42);
            case "SHORT":
                return Short.valueOf((short) 42);
            case "INT":
                return Integer.valueOf(42);
            case "LONG":
                return Long.valueOf(42L);
            case "STRING_NUMERIC":
                return "42";
            case "STRING_NUMERIC_WITH_SPACES":
                return "  42  ";
            default:
                throw new IllegalArgumentException(kind);
        }
    }
}


