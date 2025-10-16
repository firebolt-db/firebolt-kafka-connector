package com.firebolt.kafka.connect;

import java.util.Set;
import org.apache.kafka.connect.sink.SinkRecord;

public interface AbstractFireboltRecord {

    String getTableName();

    String getTopic();

    int getPartition();

    long getOffset();

    long getTimestamp();

    boolean hasValueSchema();

    Set<String> getColumnNames();

    KafkaMessageColumnValue getColumnValue(String columnName);

    SinkRecord getSinkRecord();
}
