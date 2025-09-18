package com.firebolt.kafka.connect.datatype.converter.exception;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

/**
 * Should be raised when cannot convert the kafka message to a firebolt row for data conversion reasons. It can be treated as a non recoverable exception
 */
@Getter
@NoArgsConstructor
public class RecordConversionFailedException extends RuntimeException {

    /**
     * The table name where the row should have been inserted if the conversion was successful.
     */
    private String tableName;

    /**
     * the topic name where the message originated
     */
    private String topicName;

    /**
     * The partition of the kafka message
     */
    private int kafkaPartition;

    /**
     * The offset of the kafka message
     */
    private long kafkaOffset;

    /**
     * In case there is a conversion problem for more than one column
     */
    private List<ColumnConversionFailedException> columnConversionExceptions;

    @Builder
    public RecordConversionFailedException(String message,
                                              String tableName,
                                              String topicName,
                                              int kafkaPartition,
                                              long kafkaOffset,
                                              List<ColumnConversionFailedException> columnConversionExceptions) {
        super(message);
        this.tableName = tableName;
        this.topicName = topicName;
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
        this.columnConversionExceptions = columnConversionExceptions;
    }
}
