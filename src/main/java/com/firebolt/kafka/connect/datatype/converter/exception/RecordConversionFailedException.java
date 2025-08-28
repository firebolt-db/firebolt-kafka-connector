package com.firebolt.kafka.connect.datatype.converter.exception;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Should be raised when cannot convert the kafka message to a firebolt row for data conversion reasons. It can be treated as a non recoverable exception
 */
@Builder
@Getter
@AllArgsConstructor
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
}
