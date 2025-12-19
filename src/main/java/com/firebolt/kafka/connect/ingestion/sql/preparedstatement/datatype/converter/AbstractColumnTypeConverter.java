package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

public abstract class AbstractColumnTypeConverter<T extends KafkaMessageColumnValue> implements ColumnDataTypeConverter<T> {

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

    protected ColumnConversionFailedException aColumnConversionFailedException(TableSchema.Column fireboltColumn, Object kafkaMessageValue) {
        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                "Cannot convert kafka message attribute to a " + fireboltColumn.getDataType() + " due to incompatible type: " + (kafkaMessageValue != null ? kafkaMessageValue.getClass().getName() : "null"));
    }

}
