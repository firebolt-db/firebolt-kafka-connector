package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

public class SchemaByteaDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemaByteaDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaByteaDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "bytea", -2, true);
    }

    @ParameterizedTest
    @CsvSource({
        "Hello World",
        "Test string",
        "123456789",
        "A simple text message",
        "A very long text message that is designed to test the performance and edge cases with larger string content",
        "  Spaced text  ",
        "XML reference like <root><element>value</element></root>",
        "URL like https://example.com/path?param=value",
        "Email like user@example.com",
        "Numbers: 123456789",
        "Mixed: Hello123World!",
        "Empty-ish:   "
    })
    void testConvertAndSetWithStringValues(String stringValue) throws SQLException {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(stringValue)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setString(1, stringValue);
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
    void testConvertAndSetWithByteArrayNonEmpty() throws SQLException {
        byte[] data = new byte[]{0x00, 0x01, 0x02};
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(data)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, data);
    }

    @Test
    void testConvertAndSetWithByteArrayEmptyConvertsToBackslashX() throws SQLException {
        byte[] empty = new byte[]{};
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(empty)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, "\\x".getBytes());
    }

    @Test
    void testConvertAndSetWithByteBuffer() throws SQLException {
        byte[] data = new byte[]{0x0A, 0x0B};
        ByteBuffer buffer = ByteBuffer.wrap(data);
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(buffer)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, data);
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagationForString() throws SQLException {
        String val = "test";
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(val)
                .build();

        doThrow(new SQLException("Database error"))
                .when(mockStatement).setString(1, val);

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }

    @Test
    void testConvertAndSetWithUnsupportedTypeThrows() {
        SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                .value(123)
                .build();

        assertThrows(com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn));
    }

    @Test
    void testConvertAndSetWithVariousStrings() throws SQLException {
        String[] samples = new String[]{
                "Special !@#$%^&*()_+-=[]{}|;':\",./<>?",
                "Newline text\nwith\nnewlines",
                "Tab text\twith\ttabs",
                "Carriage return text\rwith\rreturns",
                "JSON like {\"Key\": \"value\", \"number\": 123}",
                "Unicode text: café, naïve, résumé",
                "Moscow, Москва, столица, России, где я?"
        };

        for (String s : samples) {
            SchemaKafkaMessageColumnValue kafkaValue = SchemaKafkaMessageColumnValue.builder()
                    .value(s)
                    .build();

            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
            verify(mockStatement).setString(1, s);
        }
    }
} 