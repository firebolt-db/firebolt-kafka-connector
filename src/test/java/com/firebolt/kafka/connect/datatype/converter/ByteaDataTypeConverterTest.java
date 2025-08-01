package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class ByteaDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private ByteaDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new ByteaDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "bytea", -2, true);
    }

    @ParameterizedTest
    @CsvSource({
        "'SGVsbG8gV29ybGQ=', 'Hello World'",
        "'VGVzdCBzdHJpbmc=', 'Test string'",
        "'MTIzNDU2Nzg5', '123456789'",
        "'QSBzaW1wbGUgdGV4dCBtZXNzYWdl', 'A simple text message'",

        "'QSB2ZXJ5IGxvbmcgdGV4dCBtZXNzYWdlIHRoYXQgaXMgZGVzaWduZWQgdG8gdGVzdCB0aGUgcGVyZm9ybWFuY2UgYW5kIGVkZ2UgY2FzZXMgd2l0aCBsYXJnZXIgc3RyaW5nIGNvbnRlbnQ=', 'A very long text message that is designed to test the performance and edge cases with larger string content'",
        "'ICBTcGFjZWQgdGV4dCAg', '  Spaced text  '",
        "'WE1MIHJlZmVyZW5jZSBsaWtlIDxyb290PjxlbGVtZW50PnZhbHVlPC9lbGVtZW50Pjwvcm9vdD4=', 'XML reference like <root><element>value</element></root>'",
        "'VVJMIGxpa2UgaHR0cHM6Ly9leGFtcGxlLmNvbS9wYXRoP3BhcmFtPXZhbHVl', 'URL like https://example.com/path?param=value'",
        "'RW1haWwgbGlrZSB1c2VyQGV4YW1wbGUuY29t', 'Email like user@example.com'",

        "'TnVtYmVyczogMTIzNDU2Nzg5', 'Numbers: 123456789'",
        "'TWl4ZWQ6IEhlbGxvMTIzV29ybGQh', 'Mixed: Hello123World!'",
        "'RW1wdHktaXNoOiAgIA==', 'Empty-ish:   '"
    })
    void testConvertAndSetWithValidBase64Values(String base64Value, String expectedDecoded) throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Value)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        byte[] expectedBytes = expectedDecoded.getBytes();
        verify(mockStatement).setBytes(1, expectedBytes);
    }

    @Test
    void testConvertAndSetWithEmptyString() throws SQLException {
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value("")
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, "\\x".getBytes());
    }

    @Test
    void testConvertAndSetWithSimpleBytes() throws SQLException {
        String simpleText = "Hello World";
        String base64Encoded = Base64.getEncoder().encodeToString(simpleText.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, simpleText.getBytes());
    }

    @Test
    void testConvertAndSetWithBinaryData() throws SQLException {
        byte[] binaryData = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
        String base64Encoded = Base64.getEncoder().encodeToString(binaryData);
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, binaryData);
    }

    @Test
    void testConvertAndSetWithSQLExceptionPropagation() throws SQLException {
        String base64Value = Base64.getEncoder().encodeToString("test".getBytes());
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Value)
                .build();

        // Mock the statement to throw SQLException
        org.mockito.Mockito.doThrow(new SQLException("Database error"))
                .when(mockStatement).setBytes(1, "test".getBytes());

        SQLException exception = assertThrows(SQLException.class, () -> {
            converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);
        });

        assertEquals("Database error", exception.getMessage());
    }

    @Test
    void testConvertAndSetWithSpecialCharacters() throws SQLException {
        String specialChars = "!@#$%^&*()_+-=[]{}|;':\",./<>?";
        String base64Encoded = Base64.getEncoder().encodeToString(specialChars.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, specialChars.getBytes());
    }

    @Test
    void testConvertAndSetWithNewlines() throws SQLException {
        String textWithNewlines = "Newline text\nwith\nnewlines";
        String base64Encoded = Base64.getEncoder().encodeToString(textWithNewlines.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, textWithNewlines.getBytes());
    }

    @Test
    void testConvertAndSetWithTabs() throws SQLException {
        String textWithTabs = "Tab text\twith\ttabs";
        String base64Encoded = Base64.getEncoder().encodeToString(textWithTabs.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, textWithTabs.getBytes());
    }

    @Test
    void testConvertAndSetWithCarriageReturns() throws SQLException {
        String textWithReturns = "Carriage return text\rwith\rreturns";
        String base64Encoded = Base64.getEncoder().encodeToString(textWithReturns.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, textWithReturns.getBytes());
    }

    @Test
    void testConvertAndSetWithJSON() throws SQLException {
        String jsonText = "JSON like {\"Key\": \"value\", \"number\": 123}";
        String base64Encoded = Base64.getEncoder().encodeToString(jsonText.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, jsonText.getBytes());
    }

    @Test
    void testConvertAndSetWithSQLInjection() throws SQLException {
        String sqlInjection = "SQLinjection: SELECT * FROM foo BAR WHERE baz = 'banana'; DROP TABLE users;";
        String base64Encoded = Base64.getEncoder().encodeToString(sqlInjection.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, sqlInjection.getBytes());
    }

    @Test
    void testConvertAndSetWithUnicodeCharacters() throws SQLException {
        String unicodeText = "Unicode text: café, naïve, résumé";
        String base64Encoded = Base64.getEncoder().encodeToString(unicodeText.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, unicodeText.getBytes());
    }

    @Test
    void testConvertAndSetWithRussianText() throws SQLException {
        String russianText = "Moscow, Москва, столица, России, где я?";
        String base64Encoded = Base64.getEncoder().encodeToString(russianText.getBytes());
        
        KafkaMessageColumnValue kafkaValue = KafkaMessageColumnValue.builder()
                .value(base64Encoded)
                .build();

        converter.convertAndSet(mockStatement, 1, kafkaValue, testColumn);

        verify(mockStatement).setBytes(1, russianText.getBytes());
    }
} 