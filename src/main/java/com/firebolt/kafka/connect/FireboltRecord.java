package com.firebolt.kafka.connect;

import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Represents a record to be written to Firebolt database.
 * Contains the table name, column values, and metadata.
 */
public class FireboltRecord implements AbstractFireboltRecord {

    private final String tableName;
    private final Map<String, ? extends KafkaMessageColumnValue> columnValues;
    private final long timestamp;
    private final SinkRecord sinkRecord;

    public FireboltRecord(String tableName,
                          Map<String, ? extends KafkaMessageColumnValue> columnValues,
                          SinkRecord sinkRecord) {
        this.tableName = tableName;
        this.columnValues = columnValues;
        this.timestamp = sinkRecord.timestamp() != null ? sinkRecord.timestamp() : System.currentTimeMillis();
        this.sinkRecord = sinkRecord;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getTopic() {
        return sinkRecord.topic();
    }

    @Override
    public int getPartition() {
        return sinkRecord.kafkaPartition() != null ? sinkRecord.kafkaPartition() : -1;
    }

    @Override
    public long getOffset() {
        return sinkRecord.kafkaOffset();
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean hasValueSchema() {
        return sinkRecord.valueSchema() != null;
    }

    @Override
    public Set<String> getColumnNames() {
        return columnValues.keySet();
    }

    @Override
    public Set<String> getColumnNamesWithNullValues() {
        return columnValues.entrySet().stream()
                .filter(entry -> entry.getValue() == null || entry.getValue().getValue() == null)
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    public KafkaMessageColumnValue getColumnValue(String columnName) {
        return columnValues.get(columnName);
    }

    @Override
    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }
}