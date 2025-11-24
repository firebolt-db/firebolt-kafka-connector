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

class SchemalessIntegerColumnDataTypeConverterTest {

    private final SchemalessIntegerColumnDataTypeConverter converter = new SchemalessIntegerColumnDataTypeConverter();
    private final TableSchema.Column intColumn = new TableSchema.Column("count", "integer", Types.INTEGER, false);

    @ParameterizedTest
    @CsvSource({
            "BYTE, 42",
            "SHORT, 42",
            "INT, 42",
            "LONG_IN_RANGE, 42",
            "STRING_NUMERIC, 42",
            "STRING_NUMERIC_WITH_SPACES, 42"
    })
    void convertsSupportedNumericRepresentations(String kind, int expected) {
        Object value = testValue(kind);
        Integer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), intColumn);
        assertEquals(expected, result.intValue());
    }

    @Test
    void longOutOfIntRangeThrows() {
        long tooLarge = (long) Integer.MAX_VALUE + 1;
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue(tooLarge), intColumn));
        assertEquals("count", ex.getColumnName());
        assertEquals("integer", ex.getColumnType());
        assertEquals("You are trying to set a long value into an int value column. That would result in data loss.", ex.getMessage());
    }

    @Test
    void stringNotNumericThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("abc"), intColumn));
        assertEquals("count", ex.getColumnName());
        assertEquals("integer", ex.getColumnType());
    }

    @Test
    void incompatibleTypeThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue(1.23d), intColumn));
        assertEquals("count", ex.getColumnName());
        assertEquals("integer", ex.getColumnType());
    }

    private static Object testValue(String kind) {
        switch (kind) {
            case "BYTE":
                return Byte.valueOf((byte) 42);
            case "SHORT":
                return Short.valueOf((short) 42);
            case "INT":
                return Integer.valueOf(42);
            case "LONG_IN_RANGE":
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


