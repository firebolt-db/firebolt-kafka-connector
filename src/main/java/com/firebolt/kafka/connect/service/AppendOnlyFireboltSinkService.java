package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.TableWriter;
import com.firebolt.kafka.connect.convert.RecordConverterFactory;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
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
import java.util.stream.Collectors;

import lombok.SneakyThrows;
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
    private RecordConverterFactory recordConverterFactory;
    private FireboltDbService fireboltDbService;
    private FireboltMetadataService fireboltMetadataService;
    private Connection connection;

    private Map<String, Set<Integer>> assignedTopicPartitions;

    // a map between a table name and the writer for that map
    private Map<String, TableWriter> tableWriterMap;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;
    private TableWriterProvider tableWriterProvider;

    AppendOnlyFireboltSinkService(SinkConfig sinkConfig, Map<String, Set<Integer>> topicPartitions, ErrorReporter errorReporter, boolean errorToleranceAll) {
        this(sinkConfig, new FireboltDbService(), new RecordConverterFactory(sinkConfig), new HashMap<>(), topicPartitions, errorReporter, errorToleranceAll, new TableWriterProvider());
    }

    @VisibleForTesting
    AppendOnlyFireboltSinkService(SinkConfig sinkConfig, FireboltDbService fireboltDbService, RecordConverterFactory recordConverterFactory, Map<String, TableWriter> tableWriterMap, Map<String, Set<Integer>> topicPartitions, ErrorReporter errorReporter, boolean errorToleranceAll, TableWriterProvider tableWriterProvider) {
        this.config = sinkConfig;
        this.fireboltDbService = fireboltDbService;
        this.recordConverterFactory = recordConverterFactory;
        this.tableWriterMap = tableWriterMap;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        try {
            this.connection = fireboltDbService.createConnection(config.getJdbcConfig());
            if (this.config.isExactlyOnce()) {
                this.fireboltMetadataService = new FireboltMetadataService(connection);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize AppendOnlyFireboltSinkService connection", e);
        }
        this.tableWriterProvider = tableWriterProvider;
    }

    @Override
    public void processRecord(Collection<SinkRecord> records, Map<String, TableSchema> tableSchemas) throws SQLException {
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
            TableSchema tableSchema = tableSchemas.get(tableName);
            if (tableSchema == null) {
                log.error("Did not find table schema for topic {}. Ignoring the record", topic);
                continue;
            }

            if (!assignedTopicPartitions.containsKey(topic)) {
                log.error("The topic {} does not have any assigned partition to this instance of Kafka Connect.", topic);
                continue;
            }

            TableWriter tableWriter = tableWriterMap.computeIfAbsent(tableName, name -> createTableWriter(topic, tableSchema));

            List<AbstractFireboltRecord> fireboltRecords = processRecordsForTopic(topic, groupedRecords, tableWriter.getProcessedPartitionOffsets());
            tableWriter.insertRecords(fireboltRecords);
        }
    }

    private TableWriter createTableWriter(String topicName, TableSchema tableSchema) {
        log.info("Creating the table writer for {}", tableSchema.getTableName());
        Optional<String> postProcessingScript = config.getPostProcessingScript(tableSchema.getTableName());

        Map<Integer, Long> lastPartitionOffsets = getLastPartitionOffsets(topicName);
        // create a dedicated connection for the table writer
        Connection tableWriterConnection = fireboltDbService.createConnection(config.getJdbcConfig());
        FireboltMetadataService tableWriterFireboltMetadataService = null;
        if (config.isExactlyOnce()) {
            tableWriterFireboltMetadataService = new FireboltMetadataService(tableWriterConnection);
        }
        if (config.isExactlyOnce() || postProcessingScript.isPresent()) {
            try {
                tableWriterConnection.setAutoCommit(false);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return tableWriterProvider.get(tableSchema, tableWriterConnection, tableWriterFireboltMetadataService, topicName, lastPartitionOffsets, errorReporter, errorToleranceAll, postProcessingScript);
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
        tableWriterMap.clear();

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ignore) {
        }

        log.info("AppendOnlyFireboltSinkService closed");
    }

    @SneakyThrows
    private Optional<AbstractFireboltRecord> processIndividualRecord(SinkRecord record) {
        try {
            // Convert the record to a format suitable for Firebolt
            return Optional.of(recordConverterFactory.convert(record));
        } catch (RecordConversionException e) {
            log.error("Error converting record: topic={}, partition={}, offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
            if (errorToleranceAll) {
                errorReporter.report(record, e);
                return Optional.empty();
            }
            throw e;
        }
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
     * Processes records for a specific topic combination.
     *
     * @param topic the topic identifier
     * @param records the list of records for this topic/partition
     */
    private List<AbstractFireboltRecord> processRecordsForTopic(String topic, List<SinkRecord> records, Map<Integer, Long> topicProcessedOffsets) {
        log.debug("Processing {} records for topic: {}", records.size(), topic);

        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }

        return records.stream()
                .filter(sinkRecord -> !topicProcessedOffsets.containsKey(sinkRecord.originalKafkaPartition()) || sinkRecord.originalKafkaOffset() > topicProcessedOffsets.get(sinkRecord.originalKafkaPartition())) // do not process records that have already been processed
                .map(this::processIndividualRecord)
                .filter(Optional::isPresent)  // when the sink record cannot be processed it will be retuned as empty, so only keep the ones that were successfully processed
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * For easier testing
     */
    static class TableWriterProvider {
        public TableWriter get(TableSchema tableSchema, Connection connection, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, ErrorReporter errorReporter, boolean errorToleranceAll, Optional<String> postProcessingScript) {
            return new TableWriter(tableSchema, connection, fireboltMetadataService, topicName, processedPartitionOffsets, errorReporter, errorToleranceAll, postProcessingScript);
        }
    }
}
