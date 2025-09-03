package com.firebolt.kafka.connect.datatype.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class FireboltTimestamptzConverterTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "2024-01-15T14:30:45Z",
            "2024-01-15 14:30:45Z",
            "2024-01-15T14:30:45.1Z",
            "2024-01-15 14:30:45.12Z",
            "2024-01-15T14:30:45.123Z",
            "2024-01-15 14:30:45.123456Z",
            "2024-01-15 12:30:45+00",
            "2024-01-15T12:30:45+00",
            "2024-01-15T14:30:45+00:00",
            "2024-01-15 14:30:45+02:00",
            "2024-01-15T14:30:45.123456-05:30"
    })
    void acceptsValidTimestamptzStrings(String input) {
        assertTrue(FireboltTimestamptzConverter.isValidTimestamptz(input));
        OffsetDateTime parsed = FireboltTimestamptzConverter.parseTimestamptz(input);
        assertNotNull(parsed);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "2024-01-15T14:30:45",              // missing zone
            "2024-13-15T14:30:45Z",             // invalid month
            "2024-00-15T14:30:45Z",             // invalid month
            "2024-01-32T14:30:45Z",             // invalid day
            "2024-01-15 24:00:00Z",             // invalid hour
            "not a date",
            "2024-01-15T14:30:45+2",            // invalid offset
            "2024-01-15T14:30:45+02:60"         // invalid minutes in offset
    })
    void rejectsInvalidTimestamptzStrings(String input) {
        assertFalse(FireboltTimestamptzConverter.isValidTimestamptz(input));
        assertThrows(Exception.class, () -> FireboltTimestamptzConverter.parseTimestamptz(input));
    }

    @Test
    void parsesZAsUtc() {
        OffsetDateTime odt = FireboltTimestamptzConverter.parseTimestamptz("2024-01-15T14:30:45Z");
        assertEquals(ZoneOffset.UTC, odt.getOffset());
    }

    @ParameterizedTest
    @CsvSource({
            "2024-01-15T14:30:45Z,2024,1,15,14,30,45,0",
            "2024-01-15 14:30:45.1Z,2024,1,15,14,30,45,100000000",
            "2024-01-15T14:30:45.12+02:00,2024,1,15,14,30,45,120000000",
            "2024-01-15 14:30:45.123456-05:30,2024,1,15,14,30,45,123456000",
            "2024-01-15 12:30:45+00,2024,1,15,12,30,45,0",
            "2024-01-15T12:30:45+00,2024,1,15,12,30,45,0",
            "2024-01-15 14:30:45.123456789Z,2024,1,15,14,30,45,123456789",
    })
    void parsesComponentsCorrectly(String input,
                                   int expectedYear,
                                   int expectedMonth,
                                   int expectedDay,
                                   int expectedHour,
                                   int expectedMinute,
                                   int expectedSecond,
                                   int expectedNano) {
        OffsetDateTime odt = FireboltTimestamptzConverter.parseTimestamptz(input);
        assertEquals(expectedYear, odt.getYear());
        assertEquals(expectedMonth, odt.getMonthValue());
        assertEquals(expectedDay, odt.getDayOfMonth());
        assertEquals(expectedHour, odt.getHour());
        assertEquals(expectedMinute, odt.getMinute());
        assertEquals(expectedSecond, odt.getSecond());
        assertEquals(expectedNano, odt.getNano());
    }
}


