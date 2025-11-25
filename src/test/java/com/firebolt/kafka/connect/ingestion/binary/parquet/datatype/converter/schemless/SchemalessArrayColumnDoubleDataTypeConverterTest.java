package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemalessArrayColumnDoubleDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessDoubleColumnDataTypeConverter doubleConverter = new SchemalessDoubleColumnDataTypeConverter();
    private final TableSchema.Column arrayDoubleColumn = new TableSchema.Column("vals", "array(double precision)", Types.ARRAY, false);

    @Test
    void convertsMixedElementsToDoubles() {
        arrayConverter.addConverter(Double.class, doubleConverter);
        List<Object> input = Arrays.asList(1, 2L, 3.5d, 4.25f, "5.5", " 6.75 ", null);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDoubleColumn);
        assertEquals(Arrays.asList(1.0d, 2.0d, 3.5d, 4.25d, 5.5d, 6.75d, null), result);
    }
}


