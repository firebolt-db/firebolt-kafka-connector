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

class SchemalessIntegerBinaryColumnDataTypeConverterTest {

    private final SchemalessIntegerBinaryColumnDataTypeConverter converter = new SchemalessIntegerBinaryColumnDataTypeConverter();
    private final TableSchema.Column intColumn = new TableSchema.Column("count", "integer", Types.INTEGER, false);

    @ParameterizedTest
    @CsvSource({
            "  100 ",
            "" + Integer.MAX_VALUE +"",
            "" + Integer.MIN_VALUE +""
    })
    void convertsFromString(String value) {
        Integer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), intColumn);
        assertEquals(Integer.parseInt(value), result.intValue());
    }

    @Test
    void canConvertByteValue() {
        Object value = Byte.MAX_VALUE;
        Integer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), intColumn);
        assertEquals(Byte.MAX_VALUE, result.intValue());
    }

    @Test
    void canConvertShortValue() {
        Object value = Short.MAX_VALUE;
        Integer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), intColumn);
        assertEquals(Short.MAX_VALUE, result.intValue());
    }

    @Test
    void canConvertIntValue() {
        Object value = Integer.MAX_VALUE;
        Integer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), intColumn);
        assertEquals(Integer.MAX_VALUE, result.intValue());
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
}


