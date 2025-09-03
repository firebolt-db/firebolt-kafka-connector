package com.firebolt.kafka.connect;

import java.util.Map;
import lombok.Data;
import lombok.Getter;
import org.apache.kafka.connect.sink.SinkRecord;

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
    private final SinkRecord sinkRecord;

    public FireboltRecord(String tableName,
                          Map<String, KafkaMessageColumnValue> columnValues,
                          String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          SinkRecord sinkRecord) {
        this.tableName = tableName;
        this.columnValues = columnValues;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.sinkRecord = sinkRecord;
    }

}