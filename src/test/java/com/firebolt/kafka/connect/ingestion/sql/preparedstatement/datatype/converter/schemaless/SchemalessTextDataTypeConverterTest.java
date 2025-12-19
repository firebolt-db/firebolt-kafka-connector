package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import org.mockito.ArgumentCaptor;
import java.util.LinkedHashMap;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class SchemalessTextDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessTextDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessTextDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "text", 12, true);
    }

    @ParameterizedTest
    @CsvSource({
        "'hello world'",
        "'Hello World'",
        "'HELLO WORLD'",
        "'123'",
        "'text with spaces'",
        "'text-with-hyphens'",
        "'text_with_underscores'",
        "'text.with.dots'",
        "'text@with@symbols'",
        "'text#with#special#chars'",
        "'text$with$dollar$signs'",
        "'text%with%percentages'",
        "'text&with&ampersands'",
        "'text*with*asterisks'",
        "'text+with+plus+signs'",
        "'text=with=equals=signs'",
        "'text|with|pipes'",
        "'text\\with\\backslashes'",
        "'text/with/slashes'",
        "'text:with:colons'",
        "'text;with;semicolons'",
        "'text<with>brackets'",
        "'text(with)parentheses'",
        "'text[with]square[brackets]'",
        "'text{with}curly{braces}'",
        "'text\"with\"quotes'",
        "'''''text with single quotes'''''",
        "'text\nwith\nnewlines'",
        "'text\twith\ttabs'",
        "'text\rwith\rcarriage\rreturns'",
        "'   text with leading spaces'",
        "'text with trailing spaces   '",
        "'   text with both leading and trailing spaces   '",
        "'multi\nline\ntext\nwith\nnewlines'",
        "'JSON like {\"key\": \"value\", \"number\": 123}'",
        "'XML like <root><element>value</element></root>'",
        "'URL like https://example.com/path?param=value'",
        "'Email like user@example.com'",
        "'Unicode: café, naïve, résumé, Москва'",
        "'Numbers: 123456789'",
        "'Mixed: Hello123World!'",
        "'Empty-ish:   '",
        "'SQLinjection: SELECT * FROM table; DROP TABLE users;'",
        "simple",
        "with spaces",
        "with\nnewlines",
        "with\ttabs",
        "with special chars: !@#$%^&*()",
        "with unicode: é, ñ, 中文, العربية",
        "with quotes: \"double\" and 'single'",
        "with escaped chars: \\n \\t \\r \\\"",
        "123 numeric string 456",
        "mixed CASE String",
        "     padded string     "
    })
    void testConvertAndSetWithValidStringValues(String stringValue) throws SQLException {
        String actualValue = stringValue.substring(1, stringValue.length() - 1);

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(actualValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, actualValue);
    }

    @Test
    void testConvertAndSetWithEmptyString() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value("")
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, "");
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagation() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value("test value")
                .build();

        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setString(1, "test value");

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }

    @Test
    void testConvertAndSetWithLargeString() throws SQLException {
        StringBuilder largeString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeString.append("A");
        }
        String expectedValue = largeString.toString();

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(expectedValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, expectedValue);
    }

    @Test
    void testConvertAndSetWithMapValueSerializesAsJson() throws Exception {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("key", "v1");
        value.put("num", 123);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("inner", true);
        value.put("nested", nested);

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockStatement).setString(eq(1), jsonCaptor.capture());

        String json = jsonCaptor.getValue();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> parsed = mapper.readValue(json, new TypeReference<Map<String, Object>>(){});
        assertEquals("v1", parsed.get("key"));
        assertEquals(123, parsed.get("num"));
        Map<String, Object> parsedNested = mapper.convertValue(parsed.get("nested"), new TypeReference<Map<String, Object>>(){});
        assertEquals(true, parsedNested.get("inner"));
    }

    @Test
    void testConvertAndSetWithNullValueSetsLiteralNullString() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(null)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, "null");
    }

    @Test
    void testConvertAndSetWithNonMapIntegerSetsStringValue() throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(42)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, "42");
    }

    @Test
    void testConvertAndSetWithSelfReferentialMapThrowsColumnConversionFailedException() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("self", value);

        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(value)
                .build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn)
        );
    }
}