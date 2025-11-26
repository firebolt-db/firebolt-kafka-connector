package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemalessArrayColumnTextDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessTextColumnDataTypeConverter textConverter = new SchemalessTextColumnDataTypeConverter();
    private final TableSchema.Column arrayTextColumn = new TableSchema.Column("txts", "array(text)", Types.ARRAY, false);

    @Test
    void convertsMixedElementsToStrings() throws Exception {
        arrayConverter.addConverter(String.class, textConverter);

        Map<String, Object> m = new HashMap<>();
        m.put("a", 1);
        m.put("b", "x");

        List<Object> input = Arrays.asList("s", 7, m, null);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayTextColumn);

        ObjectMapper om = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> back = om.readValue((String) result.get(2), Map.class);
        assertEquals("s", result.get(0));
        assertEquals("7", result.get(1));
        assertEquals(m, back);
        assertEquals(null, result.get(3));
    }
}


