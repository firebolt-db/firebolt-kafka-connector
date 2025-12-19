package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaTextBinaryColumnDataTypeConverterTest {

    private final SchemaTextBinaryColumnDataTypeConverter converter = new SchemaTextBinaryColumnDataTypeConverter();
    private final TableSchema.Column textColumn = new TableSchema.Column("txt", "text", Types.VARCHAR, false);

    @Test
    void serializesMapAsJson() throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("a", 1);
        m.put("b", "x");

        String json = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(m)
                .build(), textColumn);
        ObjectMapper om = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> back = om.readValue(json, Map.class);
        assertEquals(m, back);
    }

    @Test
    void convertsString() {
        String s = "hello";
        String res = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(s)
                .build(), textColumn);
        assertEquals(s, res);
    }

    @Test
    void convertsNumberToString() {
        String res = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT32)
                .value(42)
                .build(), textColumn);
        assertEquals("42", res);
    }
}


