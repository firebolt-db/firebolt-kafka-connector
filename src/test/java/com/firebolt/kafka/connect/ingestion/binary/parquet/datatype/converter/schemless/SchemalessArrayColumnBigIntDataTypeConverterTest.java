package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessArrayColumnBigIntDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessBigIntColumnDataTypeConverter bigIntConverter = new SchemalessBigIntColumnDataTypeConverter();
    private final TableSchema.Column arrayBigintColumn = new TableSchema.Column("counts", "array(bigint)", Types.ARRAY, false);

    @Test
    void convertsMixedNumericElementsToLongs() {
        arrayConverter.addConverter(Long.class, bigIntConverter);
        List<Object> input = Arrays.asList(1, 2L, "3", " 4 ", null, (short) 5, (byte) 6);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayBigintColumn);
        assertEquals(Arrays.asList(1L, 2L, 3L, 4L, null, 5L, 6L), result);
    }

    @Test
    void invalidElementCausesFailure() {
        arrayConverter.addConverter(Long.class, bigIntConverter);
        List<Object> input = Arrays.asList(1, "not-a-number", 3);
        assertThrows(ColumnConversionFailedException.class,
                () -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayBigintColumn));
    }
}


