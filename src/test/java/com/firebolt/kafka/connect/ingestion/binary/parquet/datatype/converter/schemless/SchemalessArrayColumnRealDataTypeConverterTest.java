package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessArrayColumnRealDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessRealColumnDataTypeConverter realConverter = new SchemalessRealColumnDataTypeConverter();
    private final TableSchema.Column arrayRealColumn = new TableSchema.Column("amounts", "array(real)", Types.ARRAY, false);

    @Test
    void convertsMixedElementsToFloats() {
        arrayConverter.addConverter(FireboltColumnDataType.REAL, realConverter);
        List<Object> input = Arrays.asList(1, 2L, 3.5d, 4.25f, "5.5", " 6.75 ", null);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayRealColumn);
        assertEquals(Arrays.asList(1.0f, 2.0f, 3.5f, 4.25f, 5.5f, 6.75f, null), result);
    }

    @Test
    void invalidElementCausesFailure() {
        arrayConverter.addConverter(FireboltColumnDataType.REAL, realConverter);
        List<Object> input = Arrays.asList("not-a-float");
        assertThrows(ColumnConversionFailedException.class,
                () -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayRealColumn));
    }
}


