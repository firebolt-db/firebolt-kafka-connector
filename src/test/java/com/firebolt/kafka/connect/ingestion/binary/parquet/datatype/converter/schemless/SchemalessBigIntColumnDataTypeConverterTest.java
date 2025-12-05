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
            "  200 ",
            "" + Long.MAX_VALUE +"",
            "" + Long.MIN_VALUE +""
    })
    void convertsFromString(String value) {
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), bigintColumn);
        assertEquals(Long.parseLong(value), result.longValue());
    }

    @Test
    void canConvertByteValue() {
        Object value = Byte.MAX_VALUE;
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), bigintColumn);
        assertEquals(Byte.MAX_VALUE, result.longValue());
    }

    @Test
    void canConvertShortValue() {
        Object value = Short.MAX_VALUE;
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), bigintColumn);
        assertEquals(Short.MAX_VALUE, result.longValue());
    }

    @Test
    void canConvertIntValue() {
        Object value = Integer.MAX_VALUE;
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), bigintColumn);
        assertEquals(Integer.MAX_VALUE, result.longValue());
    }

    @Test
    void canConvertLongValue() {
        Object value = Long.MAX_VALUE;
        Long result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), bigintColumn);
        assertEquals(Long.MAX_VALUE, result.longValue());
    }

    @Test
    void stringNotNumericThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("abc"), bigintColumn));
        assertEquals("count64", ex.getColumnName());
        assertEquals("bigint", ex.getColumnType());
    }

}


