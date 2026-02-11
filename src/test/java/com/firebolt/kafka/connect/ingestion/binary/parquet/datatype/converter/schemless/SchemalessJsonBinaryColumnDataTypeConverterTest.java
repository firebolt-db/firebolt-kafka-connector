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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SchemalessJsonBinaryColumnDataTypeConverterTest {

    private final SchemalessJsonBinaryColumnDataTypeConverter converter = new SchemalessJsonBinaryColumnDataTypeConverter();
    private final TableSchema.Column jsonColumn = new TableSchema.Column("data", "json", Types.OTHER, false);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void convertsNullToJsonNull() {
        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(null), jsonColumn);
        assertNull(result);
    }

    @Test
    void convertsMapToJson() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        map.put("number", 42);

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(map), jsonColumn);

        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(json, Map.class);
        assertEquals(map, parsed);
    }

    @Test
    void convertsNestedMapToJson() throws Exception {
        Map<String, Object> nested = new HashMap<>();
        nested.put("b", "c");

        Map<String, Object> map = new HashMap<>();
        map.put("a", nested);

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(map), jsonColumn);
        assertEquals("{\"a\":{\"b\":\"c\"}}", json);
    }

    @Test
    void convertsListToJsonArray() throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(list), jsonColumn);
        assertEquals("[1,2,3,4]", json);
    }

    @Test
    void convertsEmptyListToEmptyJsonArray() {
        List<Object> emptyList = Arrays.asList();

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(emptyList), jsonColumn);
        assertEquals("[]", json);
    }

    @Test
    void convertsEmptyMapToEmptyJsonObject() {
        Map<String, Object> emptyMap = new HashMap<>();

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(emptyMap), jsonColumn);
        assertEquals("{}", json);
    }

    @Test
    void preservesStringAsIs() {
        String jsonString = "{\"already\":\"json\"}";

        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(jsonString), jsonColumn);
        assertEquals(jsonString, result);
    }

    @Test
    void preservesJsonLiteralStringNull() {
        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue("null"), jsonColumn);
        assertEquals("null", result);
    }

    @Test
    void preservesJsonLiteralStringTrue() {
        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue("true"), jsonColumn);
        assertEquals("true", result);
    }

    @Test
    void preservesJsonLiteralStringFalse() {
        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue("false"), jsonColumn);
        assertEquals("false", result);
    }

    @ParameterizedTest
    @CsvSource({
            "0, 0",
            "128, 128",
            "32768, 32768",
            "2147483648, 2147483648",
            "3.1415926, 3.1415926"
    })
    void convertsNumberToString(String input, String expected) {
        Object value;
        if (input.contains(".")) {
            value = Double.parseDouble(input);
        } else {
            value = Long.parseLong(input);
        }

        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), jsonColumn);
        assertEquals(expected, result);
    }

    @Test
    void convertsBooleanTrueToString() {
        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(true), jsonColumn);
        assertEquals("true", result);
    }

    @Test
    void convertsBooleanFalseToString() {
        String result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(false), jsonColumn);
        assertEquals("false", result);
    }

    @Test
    void convertsComplexNestedStructure() throws Exception {
        Map<String, Object> inner = new HashMap<>();
        inner.put("c", "d");

        List<Object> innerList = Arrays.asList("b", inner);

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put("a", innerList);

        List<Object> outerList = Arrays.asList(outerMap, Arrays.asList("e", "f"));

        String json = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(outerList), jsonColumn);

        List<?> parsed = objectMapper.readValue(json, List.class);
        assertEquals(2, parsed.size());
    }
}
