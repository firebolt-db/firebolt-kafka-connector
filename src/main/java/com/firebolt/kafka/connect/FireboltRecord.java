package com.firebolt.kafka.connect;

import java.util.Map;
import lombok.Data;
import lombok.Getter;

/**
 * Represents a record to be written to Firebolt database.
 * Contains the table name, column values, and metadata.
 */
@Data
@Getter
public class FireboltRecord {

    private final String tableName;
    private final Map<String, KafkaMessageColumnValue> columnValues;
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;

    public FireboltRecord(String tableName,
                          Map<String, KafkaMessageColumnValue> columnValues,
                          String topic,
                          int partition,
                          long offset,
                          long timestamp) {
        this.tableName = tableName;
        this.columnValues = columnValues;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

}