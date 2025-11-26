package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemalessTextColumnDataTypeConverterTest {

    private final SchemalessTextColumnDataTypeConverter converter = new SchemalessTextColumnDataTypeConverter();
    private final TableSchema.Column textColumn = new TableSchema.Column("txt", "text", Types.VARCHAR, false);

    @Test
    void serializesMapAsJson() throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("a", 1);
        m.put("b", "x");

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(m), textColumn);
        ObjectMapper om = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> back = om.readValue(json, Map.class);
        assertEquals(m, back);
    }

    @Test
    void convertsString() {
        String s = "hello";
        String res = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(s), textColumn);
        assertEquals(s, res);
    }

    @Test
    void convertsNumberToString() {
        String res = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(42), textColumn);
        assertEquals("42", res);
    }
}


