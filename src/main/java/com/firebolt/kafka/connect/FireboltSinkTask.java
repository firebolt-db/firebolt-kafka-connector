package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltSinkService;
import com.firebolt.kafka.connect.service.FireboltSinkServiceProvider;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
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

    private static final Set<ExceptionType> RETRIABLE_ERRORS = EnumSet.of(TOO_MANY_REQUESTS, CANCELED, ERROR, CONFLICT);

    private FireboltSinkService fireboltSinkService;
    private SinkConfig sinkConfig;
    private Map<String, Set<Integer>> assignedTopicPartitions;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Firebolt Sink Task: {}", props.get(TASK_ID_ATTRIBUTE));

        try {
            this.sinkConfig = new SinkConfig(props);

            this.assignedTopicPartitions = new HashMap<>();

            this.errorToleranceAll = this.sinkConfig.isErrorToleranceAll();
            createAndSetErrorReporter();

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
            assignedTopicPartitions.clear();
            for (TopicPartition partition : partitions) {
                assignedTopicPartitions.computeIfAbsent(partition.topic(), t -> new HashSet<>()).add(partition.partition());
            }

            // open method might get called on partition rebalancing. It might be that start method does not get called.
            // We need to move the firebolSinkService creation here, since we need to know which partitions will the service handle
            if (fireboltSinkService != null) {
                fireboltSinkService.close();
            }

            this.fireboltSinkService = FireboltSinkServiceProvider.getInstance().getService(sinkConfig, this.assignedTopicPartitions, this.errorReporter, this.errorToleranceAll);

            log.info("Opened Firebolt Sink Task for topic partitions: {}", assignedTopicPartitions);

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
            fireboltSinkService.processRecord(records);
            log.debug("DEBUG: fireboltSinkService.processRecord() completed successfully");
        } catch (Exception batchException) {
            log.error("Error processing records", batchException);
            handleError(batchException, records);
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
        throw new RuntimeException(String.format("Number of records that failed: %d", records.size()), batchException);
    }

    /** Transient Firebolt failures are retried by Kafka Connect; everything else fails the task. */
    private static boolean isRetriable(Throwable throwable) {
        return throwable instanceof FireboltException
                && RETRIABLE_ERRORS.contains(((FireboltException) throwable).getType());
    }
}