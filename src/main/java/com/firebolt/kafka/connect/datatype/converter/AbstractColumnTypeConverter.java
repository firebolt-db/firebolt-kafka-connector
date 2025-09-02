package com.firebolt.kafka.connect.datatype.converter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

public abstract class AbstractColumnTypeConverter implements ColumnDataTypeConverter {

    private static final DateTimeFormatter ISO_DATE_STRICT =
            DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.STRICT);

    /** true only for strings like 2024-01-23; rejects 2024-01-34, 2024-1-3, etc. */
    protected boolean isIsoLocalDate(String s) {
        if (s == null) return false;
        try {
            LocalDate.parse(s, ISO_DATE_STRICT);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

}
