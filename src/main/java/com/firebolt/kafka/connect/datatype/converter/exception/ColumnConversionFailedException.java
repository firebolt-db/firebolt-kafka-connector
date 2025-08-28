package com.firebolt.kafka.connect.datatype.converter.exception;

import lombok.Getter;

/**
 * When cannot safely convert from the kafka message attribute to the column data type in firebolt. (e.g we cannot instert "abc" into an integer column type"
 */
@Getter
public class ColumnConversionFailedException extends RuntimeException {

    /**
     * The column name for which we could not convert the value from kafka message
     */
    private String columnName;

    /**
     * The column type in firebolt
     */
    private String columnType;

    public ColumnConversionFailedException(String columnName, String columnType, String message) {
        super(message);
        this.columnName = columnName;
        this.columnType = columnType;
    }

}
