package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.firebolt.kafka.connect.service.FireboltMetadataService;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A class that knows how to insert into a Firebolt table . It will use prepared statements to do the inserts.
 * It also tracks what were the last offsets that were written for a particular partition
 */
@Slf4j
public class TableWriter {

    /**
     * For which table this write is for
     */
    private TableSchema tableSchema;

    // when this table writer is created we should fetch the offsets from the metadata table
    private Map<Integer, Long> processedPartitionOffsets;

    private InsertPreparedStatementProvider insertPreparedStatementProvider;
    private ErrorReporter errorReporter;
    private FireboltMetadataService fireboltMetadataService;
    private String topicName;
    private boolean errorToleranceAll;

    /**
     * A connection that will be used for pushing the records to firebolt table
     * It is expected that this connection will always be open if this writer is used
     * and it will be closed when the writer is closed and the writer is not used anymore
     */
    private Connection connection;

    private Optional<String> postProcessingScript;

    public TableWriter(TableSchema tableSchema, Connection connection, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, ErrorReporter errorReporter, boolean errorToleranceAll, Optional<String> postProcessingScript) {
        this(tableSchema, connection, fireboltMetadataService, topicName, processedPartitionOffsets, new InsertPreparedStatementProvider(), errorReporter, errorToleranceAll, postProcessingScript);
    }

    @VisibleForTesting
    TableWriter(TableSchema tableSchema, Connection connection, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, InsertPreparedStatementProvider insertPreparedStatementProvider, ErrorReporter errorReporter, boolean errorToleranceAll, Optional<String> postProcessingScript) {
        this.tableSchema = tableSchema;
        this.connection = connection;
        this.processedPartitionOffsets = processedPartitionOffsets;
        this.insertPreparedStatementProvider = insertPreparedStatementProvider;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        this.fireboltMetadataService = fireboltMetadataService;
        this.topicName = topicName;
        this.postProcessingScript = postProcessingScript;
    }

    public void insertRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        log.debug("Processing {} records for table: {}", fireboltRecords.size(), tableSchema.getTableName());

        if (CollectionUtils.isEmpty(fireboltRecords)) {
            return;
        }

        try {
            // Auto-commit/commit will be managed by the service when exactly-once or post-processing scripts are enabled
            InsertPreparedStatement insertPreparedStatement = insertPreparedStatementProvider.get(connection, tableSchema, errorReporter, errorToleranceAll, postProcessingScript);
            insertPreparedStatement.addRecords(fireboltRecords);

            // update the processed offsets in Kafka node
            updateProcessedOffsets(fireboltRecords);
            // Commit is only meaningful when exactly-once is enabled and the service set auto-commit to false.
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException ex) {
            if (!connection.getAutoCommit()) {
                log.error("There was an error so rolling back the transaction: {}", ex.getMessage());
                connection.rollback();
            }
            throw ex;
        }
    }

    public Map<Integer, Long> getProcessedPartitionOffsets() {
        return Collections.unmodifiableMap(processedPartitionOffsets);
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("Failed to gracefully close connection");
            }
        }
    }

    private void updateProcessedOffsets(List<AbstractFireboltRecord> fireboltRecords) {
        fireboltRecords.forEach(fireboltRecord -> {
            Integer partition = fireboltRecord.getPartition();
            Long offset = fireboltRecord.getOffset();
            if (processedPartitionOffsets.get(partition) < offset) {
                processedPartitionOffsets.put(partition, offset);
            }
        });
        if (fireboltMetadataService != null) {
            fireboltMetadataService.updateOffsets(topicName, processedPartitionOffsets);
        }
    }

    /**
     * For easier testing
     */
    static class InsertPreparedStatementProvider {
        public InsertPreparedStatement get(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, boolean errorToleranceAll, Optional<String> postProcessingScript) {
            return postProcessingScript == null || postProcessingScript.isEmpty() ? new InsertPreparedStatement(connection, tableSchema, errorReporter, errorToleranceAll)
                    : new InsertPreparedStatementWithPostProcessing(connection, tableSchema, errorReporter, errorToleranceAll, postProcessingScript.get());
        }
    }
}
