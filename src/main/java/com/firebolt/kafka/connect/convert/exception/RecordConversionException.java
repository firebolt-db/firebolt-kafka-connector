package com.firebolt.kafka.connect.convert.exception;

/**
 * Custom exception for record conversion errors.
 */
public class RecordConversionException extends Exception {

    public RecordConversionException(String message) {
        super(message);
    }

}