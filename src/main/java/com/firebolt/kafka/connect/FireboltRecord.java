package com.firebolt.kafka.connect;

import lombok.Data;
import lombok.Getter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Represents a record to be written to Firebolt database.
 * Contains the table name, column values, and metadata.
 */
@Data
public class FireboltRecord {

    @Getter
    private final String tableName;
    @Getter
    private final Map<String, KafkaMessageColumnValue> columnValues;
    private final long timestamp;
    @Getter
    private final SinkRecord sinkRecord;

    public FireboltRecord(String tableName,
                          Map<String, KafkaMessageColumnValue> columnValues,
                          SinkRecord sinkRecord) {
        this.tableName = tableName;
        this.columnValues = columnValues;
        this.timestamp = sinkRecord.timestamp() != null ? sinkRecord.timestamp() : System.currentTimeMillis();
        this.sinkRecord = sinkRecord;
    }

    public String getTopic() {
        return sinkRecord.topic();
    }

    public int getPartition() {
        return sinkRecord.kafkaPartition() != null ? sinkRecord.kafkaPartition() : -1;
    }

    public long getOffset() {
        return sinkRecord.kafkaOffset();
    }

    public long getTimestamp() {
        return timestamp;
    }
}