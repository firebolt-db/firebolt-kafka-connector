package com.firebolt.kafka.connect.ingestion.upload;

/**
 * A record could not be converted to the Avro/Parquet representation. Non-retriable:
 * the record itself is the problem, so it is either reported to the DLQ (when error
 * tolerance is enabled) or fails the task.
 */
public class RecordConversionException extends RuntimeException {

    public RecordConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordConversionException(String message) {
        super(message);
    }
}
