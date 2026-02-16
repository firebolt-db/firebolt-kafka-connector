package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SchemalessJsonDataTypeConverterTest {

    private final SchemalessJsonDataTypeConverter converter = new SchemalessJsonDataTypeConverter();
    private final TableSchema.Column jsonColumn = new TableSchema.Column("data", "json", Types.OTHER, false);

    @Mock
    private PreparedStatement preparedStatement;

    @Test
    void convertsNullToJsonNull() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(null), jsonColumn);
        verify(preparedStatement).setString(1, null);
    }

    @Test
    void convertsMapToJson() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");

        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(map), jsonColumn);
        verify(preparedStatement).setString(1, "{\"key\":\"value\"}");
    }

    @Test
    void convertsNestedMapToJson() throws Exception {
        Map<String, Object> nested = new HashMap<>();
        nested.put("b", "c");

        Map<String, Object> map = new HashMap<>();
        map.put("a", nested);

        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(map), jsonColumn);
        verify(preparedStatement).setString(1, "{\"a\":{\"b\":\"c\"}}");
    }

    @Test
    void convertsListToJsonArray() throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);

        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(list), jsonColumn);
        verify(preparedStatement).setString(1, "[1,2,3,4]");
    }

    @Test
    void convertsEmptyListToEmptyJsonArray() throws Exception {
        List<Object> emptyList = Arrays.asList();

        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(emptyList), jsonColumn);
        verify(preparedStatement).setString(1, "[]");
    }

    @Test
    void convertsEmptyMapToEmptyJsonObject() throws Exception {
        Map<String, Object> emptyMap = new HashMap<>();

        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(emptyMap), jsonColumn);
        verify(preparedStatement).setString(1, "{}");
    }

    @Test
    void preservesStringAsIs() throws Exception {
        String jsonString = "{\"already\":\"json\"}";

        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(jsonString), jsonColumn);
        verify(preparedStatement).setString(1, jsonString);
    }

    @Test
    void preservesJsonLiteralStringNull() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue("null"), jsonColumn);
        verify(preparedStatement).setString(1, "null");
    }

    @Test
    void preservesJsonLiteralStringTrue() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue("true"), jsonColumn);
        verify(preparedStatement).setString(1, "true");
    }

    @Test
    void preservesJsonLiteralStringFalse() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue("false"), jsonColumn);
        verify(preparedStatement).setString(1, "false");
    }

    @ParameterizedTest
    @CsvSource({
            "0, 0",
            "128, 128",
            "32768, 32768"
    })
    void convertsIntegerToString(int input, String expected) throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(input), jsonColumn);
        verify(preparedStatement).setString(1, expected);
    }

    @ParameterizedTest
    @CsvSource({
            "2147483648, 2147483648",
            "9223372036854775807, 9223372036854775807"
    })
    void convertsLongToString(long input, String expected) throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(input), jsonColumn);
        verify(preparedStatement).setString(1, expected);
    }

    @Test
    void convertsDoubleToString() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(3.1415926), jsonColumn);
        verify(preparedStatement).setString(1, "3.1415926");
    }

    @Test
    void convertsBooleanTrueToString() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(true), jsonColumn);
        verify(preparedStatement).setString(1, "true");
    }

    @Test
    void convertsBooleanFalseToString() throws Exception {
        converter.convertAndSet(preparedStatement, 1, new SchemalessKafkaMessageColumnValue(false), jsonColumn);
        verify(preparedStatement).setString(1, "false");
    }
}
