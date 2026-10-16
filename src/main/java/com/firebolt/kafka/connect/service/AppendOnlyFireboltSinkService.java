package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableWriter;
import com.google.common.annotations.VisibleForTesting;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import com.firebolt.kafka.connect.reporter.ErrorReporter;

/**
 * Append-only implementation of FireboltSinkService.
 * This service handles inserting records into Firebolt database tables.
 */
@Slf4j
public class AppendOnlyFireboltSinkService implements FireboltSinkService {

    private SinkConfig config;
    private FireboltDbService fireboltDbService;
    private FireboltMetadataService fireboltMetadataService;

    private Map<String, Set<Integer>> assignedTopicPartitions;

    // a map between a table name and the writer for that map
    private Map<String, TableWriter> tableWriterMap;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;
    private TableWriterProvider tableWriterProvider;

    AppendOnlyFireboltSinkService(SinkConfig sinkConfig, Map<String, Set<Integer>> topicPartitions, ErrorReporter errorReporter, boolean errorToleranceAll) {
        this(sinkConfig, new FireboltDbService(), new HashMap<>(), topicPartitions, errorReporter, errorToleranceAll, new TableWriterProvider());
    }

    @VisibleForTesting
    AppendOnlyFireboltSinkService(SinkConfig sinkConfig, FireboltDbService fireboltDbService, Map<String, TableWriter> tableWriterMap, Map<String, Set<Integer>> topicPartitions, ErrorReporter errorReporter, boolean errorToleranceAll, TableWriterProvider tableWriterProvider) {
        this.config = sinkConfig;
        this.fireboltDbService = fireboltDbService;
        this.tableWriterMap = tableWriterMap;
        this.assignedTopicPartitions = topicPartitions;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        if (this.config.isExactlyOnce()) {
            this.fireboltMetadataService = new FireboltMetadataService(fireboltDbService, config.getJdbcConfig());
        }
        this.tableWriterProvider = tableWriterProvider;
    }

    @Override
    public void processRecord(Collection<SinkRecord> records) throws SQLException {
        if (CollectionUtils.isEmpty(records)) {
            log.debug("No records to process");
            return;
        }

        // Group records by topic/partition combination
        Map<String, List<SinkRecord>> recordsByTopic = groupRecordsByTopic(records);

        log.debug("Grouped {} records into {} topic combinations", records.size(), recordsByTopic.size());

        // Process each topic separately
        for (Map.Entry<String, List<SinkRecord>> entry : recordsByTopic.entrySet()) {
            String topic = entry.getKey();
            List<SinkRecord> groupedRecords = entry.getValue();

            String tableName = config.getTableNameForTopic(topic);

            if (!assignedTopicPartitions.containsKey(topic)) {
                log.error("The topic {} does not have any assigned partition to this instance of Kafka Connect.", topic);
                continue;
            }

            TableWriter tableWriter = tableWriterMap.computeIfAbsent(tableName, name -> createTableWriter(topic, tableName));

            List<SinkRecord> unprocessedRecords = filterProcessedRecords(topic, groupedRecords, tableWriter.getProcessedPartitionOffsets());
            tableWriter.insertRecords(unprocessedRecords);
        }
    }

    private TableWriter createTableWriter(String topicName, String tableName) {
        log.info("Creating the table writer for {}", tableName);
        Optional<String> postProcessingScript = config.getPostProcessingScript(tableName);
        if (postProcessingScript.isPresent()) {
            log.info("Post-processing script found for table {} (length: {} chars)", tableName, postProcessingScript.get().length());
        } else {
            log.info("No post-processing script configured for table {}", tableName);
        }

        Map<Integer, Long> lastPartitionOffsets = getLastPartitionOffsets(topicName);
        Supplier<Connection> connectionSupplier = () -> fireboltDbService.createConnection(config.getJdbcConfig());
        return tableWriterProvider.get(tableName, connectionSupplier, fireboltMetadataService, topicName, lastPartitionOffsets, errorReporter, config);
    }

    // if exactly once is configured, then we need to fetch the saved offsets for each of the partition
    private Map<Integer, Long> getLastPartitionOffsets(String topicName) {
        if (!config.isExactlyOnce()) {
            // we will always pass -1 for the partitions that we managed. This means that when it starts fresh the table writer will not ignore any messages
            return assignedTopicPartitions.get(topicName)
                    .stream()
                    .collect(Collectors.toMap(partitionId -> partitionId, partitionId -> -1L));
        }

        log.info("Fetching the last committed offsets");

        return fireboltMetadataService.getLastOffsets(topicName, assignedTopicPartitions.get(topicName));
    }

    /**
     * Closes the service and releases resources.
     */
    public void close() {
        log.info("Closing AppendOnlyFireboltSinkService");

        // close all the table writers
        tableWriterMap.entrySet().forEach(entry -> entry.getValue().close());

        log.info("AppendOnlyFireboltSinkService closed");
    }

    /**
     * Groups records by topic since the records for all topics will be processed by the same writer
     *
     */
    private Map<String, List<SinkRecord>> groupRecordsByTopic(Collection<SinkRecord> records) {
        Map<String, List<SinkRecord>> groupedRecords = new HashMap<>();

        for (SinkRecord record : records) {
            String topic = record.originalTopic();
            groupedRecords.computeIfAbsent(topic, k -> new ArrayList<>()).add(record);
        }

        return groupedRecords;
    }

    /**
     * Drops records whose offsets were already ingested (exactly-once replay protection).
     *
     * @param topic the topic identifier
     * @param records the list of records for this topic/partition
     */
    private List<SinkRecord> filterProcessedRecords(String topic, List<SinkRecord> records, Map<Integer, Long> topicProcessedOffsets) {
        log.debug("Processing {} records for topic: {}", records.size(), topic);

        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }

        return records.stream()
                .filter(sinkRecord -> !topicProcessedOffsets.containsKey(sinkRecord.originalKafkaPartition()) || sinkRecord.originalKafkaOffset() > topicProcessedOffsets.get(sinkRecord.originalKafkaPartition())) // do not process records that have already been processed
                .collect(Collectors.toList());
    }
}
