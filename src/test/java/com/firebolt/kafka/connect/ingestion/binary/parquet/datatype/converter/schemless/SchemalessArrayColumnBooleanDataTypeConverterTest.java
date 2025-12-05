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

class SchemalessArrayColumnBooleanDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessBooleanColumnDataTypeConverter boolConverter = new SchemalessBooleanColumnDataTypeConverter();
    private final TableSchema.Column arrayBoolCol = new TableSchema.Column("flags", "array(boolean)", Types.ARRAY, false);

    @Test
    void convertsMixedElementsToBooleans() {
        arrayConverter.addConverter(FireboltColumnDataType.BOOLEAN, boolConverter);
        List<Object> in = Arrays.asList("true", "FALSE", 1, 0, true, false, null, "t", "f");
        List<?> out = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(in), arrayBoolCol);
        assertEquals(Arrays.asList(true, false, true, false, true, false, null, true, false), out);
    }

    @Test
    void invalidElementCausesFailure() {
        arrayConverter.addConverter(FireboltColumnDataType.BOOLEAN, boolConverter);
        List<Object> in = Arrays.asList("maybe");
        assertThrows(ColumnConversionFailedException.class,
                () -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(in), arrayBoolCol));
    }
}


