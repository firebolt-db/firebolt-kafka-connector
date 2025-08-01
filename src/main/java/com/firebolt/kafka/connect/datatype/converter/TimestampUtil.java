package com.firebolt.kafka.connect.datatype.converter;

import java.sql.Timestamp;
import java.time.Instant;

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

    private static Timestamp fromMicros(long micros) {
        long seconds = micros / 1_000_000;
        long microRemainder = micros % 1_000_000;
        Instant instant = Instant.ofEpochSecond(seconds, microRemainder * 1000);
        return Timestamp.from(instant);
    }
}
