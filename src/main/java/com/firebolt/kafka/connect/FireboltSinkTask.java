package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.ingestion.parquet.RecordConversionException;
import com.firebolt.kafka.connect.service.FireboltDbService;
import com.firebolt.kafka.connect.service.FireboltSinkService;
import com.firebolt.kafka.connect.service.FireboltSinkServiceProvider;
import com.google.common.collect.Sets;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import org.apache.kafka.connect.errors.RetriableException;

import static com.firebolt.jdbc.exception.ExceptionType.*;
import static com.firebolt.kafka.connect.reporter.ErrorReporter.nullErrorReporter;

/**
 * Firebolt Sink Task that handles the actual data processing.
 * This task receives records from Kafka topics and delegates processing to the appropriate FireboltSinkService.
 */
@Slf4j
public class FireboltSinkTask extends SinkTask {

    public static final String TASK_ID_ATTRIBUTE = "task.id";

    private FireboltSinkService fireboltSinkService;
    private SinkConfig sinkConfig;
    private Set<String> assignedTopics;
    private Map<String, String> topicToTableMapping;
    private Map<String, TableSchema> tableSchemas;
    private Map<String, Set<Integer>> assignedTopicPartitions;
    private FireboltDbService fireboltDbService;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;

    @Override
    public String version() {
        try {
            Properties properties = new Properties();
            try (InputStream input = getClass().getClassLoader().getResourceAsStream("version.properties")) {
                if (input != null) {
                    properties.load(input);
                    return properties.getProperty("version", "unknown");
                }
            }
        } catch (IOException e) {
            log.warn("Failed to load version from properties file", e);
        }
        return "unknown";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Firebolt Sink Task: {}", props.get(TASK_ID_ATTRIBUTE));

        try {
            this.sinkConfig = new SinkConfig(props);

            // Initialize collections
            this.assignedTopics = new HashSet<>();
            this.topicToTableMapping = new HashMap<>();
            this.tableSchemas = new HashMap<>();
            this.assignedTopicPartitions = new HashMap<>();

            this.errorToleranceAll = this.sinkConfig.isErrorToleranceAll();
            createAndSetErrorReporter();

            // Initialize services
            this.fireboltDbService = new FireboltDbService();

            log.info("Firebolt Sink Task started successfully");

        } catch (Exception e) {
            log.error("Failed to start Firebolt Sink Task", e);
            throw new RuntimeException("Failed to start Firebolt Sink Task", e);
        }
    }

    private void createAndSetErrorReporter() {
        this.errorReporter = nullErrorReporter();
        if (context != null) {
            try {
                ErrantRecordReporter errReporter = context.errantRecordReporter();
                if (errReporter != null) {
                    this.errorReporter = errReporter::report;
                } else {
                    log.info("Errant record reporter not configured.");
                }
            } catch (NoClassDefFoundError | NoSuchMethodError e) {
                log.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
            }
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        log.info("Opening Firebolt Sink Task for {} partitions", partitions.size());

        try {
            // Extract unique topics from the assigned partitions
            extractAssignedTopics(partitions);

            // Map topics to table names
            buildTopicToTableMapping();

            // Discover table schemas from Firebolt
            discoverTableSchemas();

            // open method might get called on partition rebalancing. It might be that start method does not get called.
            // We need to move the firebolSinkService creation here, since we need to know which partitions will the service handle
            if (fireboltSinkService != null) {
                fireboltSinkService.close();
            }

            this.fireboltSinkService = FireboltSinkServiceProvider.getInstance().getService(sinkConfig, this.assignedTopicPartitions, this.errorReporter, this.errorToleranceAll);

            log.info("Successfully opened Firebolt Sink Task for topics: {} mapped to tables: {}",
                    assignedTopics, topicToTableMapping.values());

        } catch (Exception e) {
            log.error("Failed to open Firebolt Sink Task", e);
            throw new RuntimeException("Failed to open Firebolt Sink Task", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            log.warn("FireboltSinkTask.put() called with no records to process");
            return;
        }

        log.info("Received {} records for processing", records.size());
        try {
            // Delegate to the appropriate service
            fireboltSinkService.processRecord(records, tableSchemas);
            log.debug("DEBUG: fireboltSinkService.processRecord() completed successfully");
        } catch (Exception batchException) {
            log.error("Error processing records", batchException);
            handleError(batchException, records);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.debug("Flushing records with offsets: {}", currentOffsets);

        try {
            // The service should handle flushing internally
            // For now, we don't need to do anything extra here
            log.debug("Flush completed");
        } catch (Exception e) {
            log.error("Error flushing records", e);
            throw new RuntimeException("Error flushing records", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Firebolt Sink Task");

        if (fireboltSinkService != null) {
            log.debug("Stopping the sink service");
            try {
                fireboltSinkService.close();
            } catch (Exception e) {
                log.error("Error closing Firebolt Sink Service", e);
                // Don't re-throw the exception to ensure graceful shutdown
            }
        }
    }

    /**
     * Extracts unique topic names from the assigned topic partitions.
     *
     * @param partitions the collection of topic partitions assigned to this task
     */
    private void extractAssignedTopics(Collection<TopicPartition> partitions) {
        assignedTopics.clear();
        assignedTopicPartitions.clear();

        for (TopicPartition partition : partitions) {
            assignedTopics.add(partition.topic());
            assignedTopicPartitions
                    .computeIfAbsent(partition.topic(), t -> new HashSet<>())
                    .add(partition.partition());

        }

        log.info("Extracted {} unique topics from {} partitions: {}",
                assignedTopics.size(), partitions.size(), assignedTopics);
    }

    /**
     * Builds the mapping from topics to table names using the configuration.
     */
    private void buildTopicToTableMapping() {
        topicToTableMapping.clear();

        for (String topic : assignedTopics) {
            String tableName = sinkConfig.getTableNameForTopic(topic);
            if (tableName != null) {
                topicToTableMapping.put(topic, tableName);
                log.info("Mapped topic '{}' to table '{}'", topic, tableName);
            } else {
                topicToTableMapping.put(topic, topic);
                log.info("No table mapping found for topic '{}', so mapping it to table '{}'", topic, topic);
            }
        }
    }

    /**
     * Discovers table schemas from Firebolt for all mapped tables.
     */
    private void discoverTableSchemas() {
        tableSchemas.clear();

        if (topicToTableMapping.isEmpty()) {
            log.info("No table mappings available, skipping schema discovery");
            return;
        }

        Set<String> uniqueTableNames = new HashSet<>(topicToTableMapping.values());
        try {
            JdbcConfig jdbcConfig = sinkConfig.getJdbcConfig();
            this.tableSchemas = fireboltDbService.discoverTableSchemas(jdbcConfig, uniqueTableNames);
            log.info("Successfully discovered schemas for {} tables", tableSchemas.size());
        } catch (Exception e) {
            log.error("Failed to discover table schemas", e);
            throw new RuntimeException("Failed to discover table schemas", e);
        }

        // if we did not find all the tables names from the mapping then throw an exception
        Set<String> tablesNotFoundInFirebolt = Sets.difference(uniqueTableNames, tableSchemas.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
        if (!tablesNotFoundInFirebolt.isEmpty()) {
            log.error("The following tables were not found in firebolt: {}", tablesNotFoundInFirebolt);
            throw new RuntimeException("The following tables were not found in Firebolt:" + tablesNotFoundInFirebolt.stream().collect(Collectors.joining(",")));
        }
    }

    private void handleError(Exception batchException, Collection<SinkRecord> records) {
        if (errorToleranceAll) {
            log.info("Errors tolerance is enabled, reporting to DLQ and continuing: {}", batchException.getLocalizedMessage());
            records.forEach(batchRecord -> errorReporter.report(batchRecord, batchException));
            return;
        }

        if (isRetriable(batchException)) {
            throw new RetriableException(batchException);
        }

        log.error("Non-retriable error encountered; failing the task: {}", batchException.getLocalizedMessage());
        if (records != null) {
            throw new RuntimeException(String.format("Number of records that failed: %d", records.size()), batchException);
        } else {
            throw new RuntimeException("Records were null", batchException);
        }
    }

    /**
     * Determines whether an exception is likely transient and thus retriable by Kafka Connect.
     */
    private boolean isRetriable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        if (throwable instanceof RecordConversionException) {
            return false;
        }

        if (throwable instanceof FireboltException) {
            FireboltException fe = (FireboltException) throwable;
            ExceptionType type = fe.getType();
            final List<ExceptionType> retriableExceptions = List.of(TOO_MANY_REQUESTS, CANCELED, ERROR, CONFLICT);
            final List<ExceptionType> nonRetriableExceptions = List.of(UNAUTHORIZED, TYPE_NOT_SUPPORTED, TYPE_TRANSFORMATION_ERROR, REQUEST_BODY_TOO_LARGE, INVALID_REQUEST, RESOURCE_NOT_FOUND);

            if (nonRetriableExceptions.contains(type)) {
                return false;
            }
            if (retriableExceptions.contains(type)) {
                return true;
            }
        }

        return false;
    }

}