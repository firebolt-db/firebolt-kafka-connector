package com.firebolt.kafka.connect.datatype.converter;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

/**
 * Converter for Firebolt TIMESTAMPTZ string values.
 * Supports ISO-8601/RFC-3339 local timestamp with either:
 *  - explicit zone offset (+/-HH[:mm])
 *  - literal 'Z' (UTC)
 *  - named time zone is not supported by Java parser here and should be pre-normalized by producer.
 *
 * Accepted formats:
 *  - yyyy-MM-dd[ ]HH:mm:ss[.fraction up to 9]Z
 *  - yyyy-MM-dd'T'HH:mm:ss[.fraction up to 9]Z
 *  - yyyy-MM-dd[ ]HH:mm:ss[.fraction up to 9][+/-]HH[:mm]
 *  - yyyy-MM-dd'T'HH:mm:ss[.fraction up to 9][+/-]HH[:mm]
 */
public final class FireboltTimestamptzConverter {

    private FireboltTimestamptzConverter() {}

    private static final DateTimeFormatter LDT_T_OFFSET_ID = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .appendOffsetId()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter LDT_SPACE_OFFSET_ID = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .appendOffsetId()
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter LDT_T_HH = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .appendOffset("+HH", "+00")
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter LDT_SPACE_HH = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendPattern("HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
            .appendOffset("+HH", "+00")
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    public static boolean isValidTimestamptz(String value) {
        if (value == null) {
            return false;
        }
        return tryParse(value, LDT_T_OFFSET_ID)
                || tryParse(value, LDT_SPACE_OFFSET_ID)
                || tryParse(value, LDT_T_HH)
                || tryParse(value, LDT_SPACE_HH);
    }

    public static OffsetDateTime parseTimestamptz(String value) {
        try { return OffsetDateTime.parse(value, LDT_T_OFFSET_ID); } catch (DateTimeParseException e) { /* fallthrough */ }
        try { return OffsetDateTime.parse(value, LDT_SPACE_OFFSET_ID); } catch (DateTimeParseException e) { /* fallthrough */ }
        try { return OffsetDateTime.parse(value, LDT_T_HH); } catch (DateTimeParseException e) { /* fallthrough */ }
        return OffsetDateTime.parse(value, LDT_SPACE_HH);
    }

    private static boolean tryParse(String value, DateTimeFormatter formatter) {
        try { OffsetDateTime.parse(value, formatter); return true; } catch (DateTimeParseException e) { return false; }
    }
}


