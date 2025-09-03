package com.firebolt.kafka.connect.datatype.converter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

public final class FireboltTimestampConverter {

    private FireboltTimestampConverter() {}

    public static final DateTimeFormatter ISO_LOCAL_DATE_TIME_STRICT =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .appendPattern("HH:mm:ss")
                    .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);

    // Accept a space between date and time as well (common lenient format we support)
    public static final DateTimeFormatter ISO_LOCAL_DATE_TIME_WITH_SPACE_STRICT =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .appendPattern("HH:mm:ss")
                    .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);

    // Accept UTC 'Z' suffix with 'T' separator
    public static final DateTimeFormatter ISO_LOCAL_DATE_TIME_T_Z_STRICT =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .appendPattern("HH:mm:ss")
                    .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
                    .appendLiteral('Z')
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);

    // Accept UTC 'Z' suffix with space separator
    public static final DateTimeFormatter ISO_LOCAL_DATE_TIME_SPACE_Z_STRICT =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .appendPattern("HH:mm:ss")
                    .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
                    .appendLiteral('Z')
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);

    public static boolean isIsoLocalDateTime(String s) {
        if (s == null) return false;
        try {
            LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_STRICT);
            return true;
        } catch (DateTimeParseException e) {
            try {
                LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_WITH_SPACE_STRICT);
                return true;
            } catch (DateTimeParseException e2) {
                try {
                    LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_T_Z_STRICT);
                    return true;
                } catch (DateTimeParseException e3) {
                    try {
                        LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_SPACE_Z_STRICT);
                        return true;
                    } catch (DateTimeParseException e4) {
                        return false;
                    }
                }
            }
        }
    }

    public static LocalDateTime parseIsoLocalDateTime(String s) {
        try {
            return LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_STRICT);
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_WITH_SPACE_STRICT);
            } catch (DateTimeParseException e2) {
                try {
                    return LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_T_Z_STRICT);
                } catch (DateTimeParseException e3) {
                    return LocalDateTime.parse(s, ISO_LOCAL_DATE_TIME_SPACE_Z_STRICT);
                }
            }
        }
    }
}


