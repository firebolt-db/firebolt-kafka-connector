package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.FireboltWriter;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.convert.RecordConverter;
import com.firebolt.kafka.connect.convert.RecordConverterFactory;
import com.firebolt.kafka.connect.convert.SchemaBasedRecordConverter;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Append-only implementation of FireboltSinkService.
 * This service handles inserting records into Firebolt database tables.
 */
@Slf4j
public class AppendOnlyFireboltSinkService implements FireboltSinkService {

    private SinkConfig config;
    private Connection fireboltConnection;
    private FireboltWriter fireboltWriter;
    private RecordConverterFactory recordConverterFactory;
    private FireboltDbService fireboltDbService;

    /**
     * Default constructor for service provider instantiation.
     */
    AppendOnlyFireboltSinkService(SinkConfig sinkConfig) {
        initialize(sinkConfig);
    }

    @Override
    public void processRecord(Collection<SinkRecord> records, Map<String, TableSchema> tableSchemas) {
        if (CollectionUtils.isEmpty(records)) {
            log.debug("No records to process");
            return;
        }

        try {
            // Group records by topic/partition combination
            Map<TopicPartitionKey, List<SinkRecord>> recordsByTopicPartition = groupRecordsByTopicPartition(records);

            log.debug("Grouped {} records into {} topic/partition combinations",
                    records.size(), recordsByTopicPartition.size());

            // Process each topic/partition group separately
            for (Map.Entry<TopicPartitionKey, List<SinkRecord>> entry : recordsByTopicPartition.entrySet()) {
                TopicPartitionKey topicPartition = entry.getKey();
                List<SinkRecord> groupedRecords = entry.getValue();

                log.debug("Processing {} records for topic/partition: {}",
                        groupedRecords.size(), topicPartition);

                String tableName = config.getTableNameForTopic(topicPartition.getTopic());
                TableSchema tableSchema = tableSchemas.get(tableName);
                if (tableSchema == null) {
                    log.warn("Did not find table schema for topic {}. Ignoring the record", topicPartition.getTopic());
                    continue;
                }

                processRecordsForTopicPartition(topicPartition, groupedRecords, tableSchema);
            }

            // Flush any remaining batched records that haven't reached the batch size threshold
            fireboltWriter.flush();

        } catch (Exception e) {
            log.error("Error processing records in AppendOnlyFireboltSinkService", e);
            throw new RuntimeException("Error processing records", e);
        }
    }

    /**
     * Initializes the service with configuration properties.
     * This method is called when the service is first used.
     *
     * @param sinkConfig the configuration properties
     */
    private void initialize(SinkConfig sinkConfig) {
        log.info("Initializing AppendOnlyFireboltSinkService");

        try {
            this.config = sinkConfig;

            // Initialize components
            this.fireboltDbService = new FireboltDbService();
            initializeFireboltConnection();
            this.fireboltWriter = new FireboltWriter(config, fireboltConnection);
            this.recordConverterFactory = new RecordConverterFactory(config);

            log.info("AppendOnlyFireboltSinkService initialized successfully");

        } catch (Exception e) {
            log.error("Failed to initialize AppendOnlyFireboltSinkService", e);
            throw new RuntimeException("Failed to initialize AppendOnlyFireboltSinkService", e);
        }
    }

    /**
     * Closes the service and releases resources.
     */
    public void close() {
        log.info("Closing AppendOnlyFireboltSinkService");

        try {
            // Close resources
            if (fireboltWriter != null) {
                fireboltWriter.close();
            }

            // Close connection
            if (fireboltConnection != null && !fireboltConnection.isClosed()) {
                fireboltConnection.close();
                log.info("Firebolt connection closed");
            }

        } catch (Exception e) {
            log.error("Error closing AppendOnlyFireboltSinkService", e);
        }

        log.info("AppendOnlyFireboltSinkService closed");
    }

    private void processIndividualRecord(SinkRecord record, TableSchema tableSchema) {
        try {
            log.info("DEBUG: processIndividualRecord() called for topic={}, partition={}, offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());

            // Log details about the record we're about to convert
            log.info("DEBUG: Record details - hasSchema={}, valueType={}, schemaName={}",
                    record.valueSchema() != null,
                    record.value() != null ? record.value().getClass().getSimpleName() : "null",
                    record.valueSchema() != null ? record.valueSchema().name() : "null");

            log.info("DEBUG: About to call recordConverterFactory.convert() for record");
            // Convert the record to a format suitable for Firebolt
            FireboltRecord fireboltRecord = recordConverterFactory.convert(record);
            log.info("DEBUG: recordConverterFactory.convert() completed successfully, got FireboltRecord: {}", fireboltRecord);

            log.info("DEBUG: About to write FireboltRecord to database");
            // Write to Firebolt
            fireboltWriter.write(fireboltRecord, tableSchema);
            log.info("DEBUG: fireboltWriter.write() completed successfully");

        } catch (RecordConversionException e) {
            log.error("Error converting record: topic={}, partition={}, offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
            throw new RuntimeException("Error converting record", e);
        } catch (Exception e) {
            log.error("Error processing record: topic={}, partition={}, offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);

            // For now, always throw on error since error tolerance was removed
            throw new RuntimeException("Error processing record", e);
        }
    }

    private void initializeFireboltConnection() throws ConnectionFailedException {
        log.info("Initializing Firebolt connection");

        try {
            // Create connection using FireboltDbService
            fireboltConnection = fireboltDbService.createConnection(config.getJdbcConfig());
            log.info("Successfully connected to Firebolt");

        } catch (ConnectionFailedException e) {
            log.error("Failed to connect to Firebolt", e);
            throw e;
        }
    }

    /**
     * Groups records by topic/partition combination for efficient processing.
     *
     * @param records the collection of records to group
     * @return a map where keys are TopicPartitionKey objects and values are lists of records
     */
    private Map<TopicPartitionKey, List<SinkRecord>> groupRecordsByTopicPartition(Collection<SinkRecord> records) {
        Map<TopicPartitionKey, List<SinkRecord>> groupedRecords = new HashMap<>();

        for (SinkRecord record : records) {
            TopicPartitionKey topicPartitionKey = createTopicPartitionKey(record.topic(), record.kafkaPartition());

            groupedRecords.computeIfAbsent(topicPartitionKey, k -> new ArrayList<>()).add(record);
        }

        return groupedRecords;
    }

    /**
     * Processes records for a specific topic/partition combination.
     *
     * @param topicPartition the topic/partition identifier
     * @param records the list of records for this topic/partition
     */
    private void processRecordsForTopicPartition(TopicPartitionKey topicPartition, List<SinkRecord> records, TableSchema tableSchema) {
        log.debug("Processing {} records for topic/partition: {}", records.size(), topicPartition);

        try {
            for (SinkRecord record : records) {
                processIndividualRecord(record, tableSchema);
            }

            log.debug("Successfully processed {} records for topic/partition: {}",
                    records.size(), topicPartition);

        } catch (Exception e) {
            log.error("Error processing records for topic/partition: {}", topicPartition, e);
            throw new RuntimeException("Error processing records for topic/partition: " + topicPartition, e);
        }
    }

    /**
     * Creates a consistent key for topic/partition combination.
     *
     * @param topic the topic name
     * @param partition the partition number (can be null)
     * @return a TopicPartitionKey representing the topic/partition combination
     */
    private TopicPartitionKey createTopicPartitionKey(String topic, Integer partition) {
        return new TopicPartitionKey(topic, partition);
    }

}
