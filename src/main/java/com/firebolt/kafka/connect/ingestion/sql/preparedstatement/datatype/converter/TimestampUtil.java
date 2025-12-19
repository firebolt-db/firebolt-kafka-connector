package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class TimestampUtil {

    public static Timestamp asTimestamp(Long value) {
        if (value == null) {
            return null;
        }

        if (value > 10_000_000_000_000L) {
            // Assume microseconds
            return fromMicros(value);
        } else {
            return new Timestamp(value);
        }
    }

    public static OffsetDateTime asOffsetDateTime(Long value) {
        if (value == null) {
            return null;
        }

        if (value > 10_000_000_000_000L) {
            // Assume microseconds
            return asOffsetDateTimeFromMicros(value);
        } else {
            return asOffsetDateTimeFromMillis(value);
        }
    }

    public static Date fromDaysSinceEpoch(int numberOfDays) {
        LocalDate localDate = LocalDate.ofEpochDay(numberOfDays);
        return Date.valueOf(localDate);
    }

    private static Timestamp fromMicros(long micros) {
        long seconds = micros / 1_000_000;
        long microRemainder = micros % 1_000_000;
        Instant instant = Instant.ofEpochSecond(seconds, microRemainder * 1000);
        return Timestamp.from(instant);
    }

    private static OffsetDateTime asOffsetDateTimeFromMicros(long micros) {
        long seconds = micros / 1_000_000;
        long microRemainder = micros % 1_000_000;
        Instant instant = Instant.ofEpochSecond(seconds, microRemainder * 1000);
        return instant.atOffset(ZoneOffset.ofHours(0)); // assume utc if it is in micros
    }

    private static OffsetDateTime asOffsetDateTimeFromMillis(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        return instant.atOffset(ZoneOffset.ofHours(0)); // assume utc if it is in micros
    }


}
