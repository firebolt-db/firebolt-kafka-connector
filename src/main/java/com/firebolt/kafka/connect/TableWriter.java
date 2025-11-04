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

    /**
     * A supplier that can give us a new connection, in case the current one is closed
     */
    private Supplier<Connection> connectionSupplier;

    private InsertPreparedStatementProvider insertPreparedStatementProvider;
    private ErrorReporter errorReporter;
    private FireboltMetadataService fireboltMetadataService;
    private String topicName;
    private boolean errorToleranceAll;
    private long maxQuerySize;

    /**
     * A connection that will be used for pushing the records to firebolt table
     */
    private Connection connection;

    private Optional<String> postProcessingScript;

    public TableWriter(TableSchema tableSchema, Supplier<Connection> connectionSupplier, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, ErrorReporter errorReporter, Optional<String> postProcessingScript, SinkConfig config) {
        this(tableSchema, connectionSupplier, fireboltMetadataService, topicName, processedPartitionOffsets, new InsertPreparedStatementProvider(), errorReporter, postProcessingScript, config);
    }

    @VisibleForTesting
    TableWriter(TableSchema tableSchema, Supplier<Connection> connectionSupplier, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, InsertPreparedStatementProvider insertPreparedStatementProvider, ErrorReporter errorReporter, Optional<String> postProcessingScript, SinkConfig config) {
        this.tableSchema = tableSchema;
        this.connectionSupplier = connectionSupplier;
        this.fireboltMetadataService = fireboltMetadataService;
        this.topicName = topicName;
        this.processedPartitionOffsets = processedPartitionOffsets;
        this.insertPreparedStatementProvider = insertPreparedStatementProvider;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = config.isErrorToleranceAll();
        this.postProcessingScript = postProcessingScript;
        this.maxQuerySize = config.getMaxQuerySize();
    }

    public void insertRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        log.debug("Processing {} records for table: {}", fireboltRecords.size(), tableSchema.getTableName());

        if (CollectionUtils.isEmpty(fireboltRecords)) {
            return;
        }

        InsertPreparedStatement insertPreparedStatement = insertPreparedStatementProvider.get(getConnection(), tableSchema, errorReporter, errorToleranceAll, postProcessingScript, maxQuerySize);
        insertPreparedStatement.addRecords(fireboltRecords);

        // update the processed offsets in Kafka node
        updateProcessedOffsets(fireboltRecords);
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

    }

    private Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = connectionSupplier.get();
        }

        if (connection.isClosed()) {
            log.warn("Connection is not open so create a new one");
            connection = connectionSupplier.get();
        }

        return connection;
    }

    /**
     * For easier testing
     */
    static class InsertPreparedStatementProvider {
        public InsertPreparedStatement get(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, boolean errorToleranceAll, Optional<String> postProcessingScript, long maxQuerySize) {
            return postProcessingScript == null || postProcessingScript.isEmpty() ? new InsertPreparedStatement(connection, tableSchema, errorReporter, errorToleranceAll, maxQuerySize)
                    : new InsertPreparedStatementWithPostProcessing(connection, tableSchema, errorReporter, errorToleranceAll, postProcessingScript.get(), maxQuerySize);
        }
    }
}
