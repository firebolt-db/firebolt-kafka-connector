package com.firebolt.kafka.connect.ingestion.binary.parquet;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroNameSanitizerTest {

    @ParameterizedTest
    @NullSource
    @EmptySource
    void returnsRecordForNullOrEmpty(String input) {
        AvroNameSanitizer sanitizer = new AvroNameSanitizer();
        assertEquals("record", sanitizer.toValidAvroName(input));
    }

    @ParameterizedTest
    @CsvSource({
            "abc,abc",
            "Abc123,Abc123",
            "_abc,_abc",
            "A_B,A_B",
            "___name,___name"
    })
    void leavesValidNamesUnchanged(String input, String expected) {
        AvroNameSanitizer sanitizer = new AvroNameSanitizer();
        assertEquals(expected, sanitizer.toValidAvroName(input));
    }

    @ParameterizedTest
    @CsvSource({
            "abc-123,abc_123",
            "a.b,a_b",
            "a b,a_b",
            "a$b,a_b",
            "a@b,a_b",
            "naïve,na_ve"
    })
    void replacesUnsupportedCharactersWithUnderscore(String input, String expected) {
        AvroNameSanitizer sanitizer = new AvroNameSanitizer();
        assertEquals(expected, sanitizer.toValidAvroName(input));
    }

    @ParameterizedTest
    @CsvSource({
            "1abc,_1abc",
            "9,_9",
            "123,_123",
            "0_name,_0_name"
    })
    void prefixesUnderscoreWhenFirstCharIsNotLetterOrUnderscore(String input, String expected) {
        AvroNameSanitizer sanitizer = new AvroNameSanitizer();
        assertEquals(expected, sanitizer.toValidAvroName(input));
    }

    @ParameterizedTest
    @CsvSource({
            "!!!!,record",
            "___,record",
            "___???___,record"
    })
    void returnsRecordWhenAllCharactersResolveToUnderscores(String input, String expected) {
        AvroNameSanitizer sanitizer = new AvroNameSanitizer();
        assertEquals(expected, sanitizer.toValidAvroName(input));
    }
}


