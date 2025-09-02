package com.firebolt.kafka.connect.datatype.converter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

public class FireboltTimestampConverterTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "2024-01-15T14:30:45",
            "2024-01-15T14:30:45.1",
            "2024-01-15T14:30:45.12",
            "2024-01-15T14:30:45.123",
            "2024-01-15T14:30:45.123456",
            "2024-01-15T14:30:45.123456789",
            "2024-01-15T14:30:45.123456789Z",
            "2024-01-15 14:30:45",
            "2024-01-15 14:30:45.1",
            "2024-01-15 14:30:45.12",
            "2024-01-15 14:30:45.123",
            "2024-01-15 14:30:45.123456",
            "2024-01-15 14:30:45.123456789",
            "2024-01-15 14:30:45.123456789Z"
    })
    void acceptsIsoLocalDateTime(String input) {
        assertTrue(FireboltTimestampConverter.isIsoLocalDateTime(input));
        assertDoesNotThrow(() -> FireboltTimestampConverter.parseIsoLocalDateTime(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "2024-01-15T14:30:45.",     // ends with .
            "2024-1-5T04:05:06",       // non-zero-padded month/day
            "2024-01-15T14:30",        // missing seconds
            "2024-01-15T14:30:61",     // invalid seconds
            "2024-02-30T12:00:00",     // invalid date
            "2024-01-15T14:30:45+01:00" // offset not allowed
    })
    void rejectsNonIsoLocalDateTime(String input) {
        assertFalse(FireboltTimestampConverter.isIsoLocalDateTime(input));
    }

    @Test
    void parseIsoLocalDateTimeThrowsOnInvalid() {
        assertThrows(Exception.class, () -> FireboltTimestampConverter.parseIsoLocalDateTime("not-a-date"));
    }
}


