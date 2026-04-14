package com.firebolt.kafka.connect;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Decorator that runs a user-defined post-processing SQL script in the same transaction as the data insert.
 *
 * When manageTransaction is true (the default, used for at-least-once mode), this class owns the
 * transaction: BEGIN → insert data → run script → COMMIT / ROLLBACK.
 *
 * When manageTransaction is false (used for exactly-once mode), the transaction is owned by
 * TableWriter so that the post-processing script and the offset metadata update are all committed
 * atomically in a single Firebolt transaction.
 */
@Slf4j
public class IngestionServiceWithPostProcessing implements IngestionService {

    static final String BATCH_ID_COLUMN_NAME = "batch_id";
    private static final String FIREBOLT_BATCH_ID_KEY = "firebolt_param.batch_id";

    private final IngestionService ingestionService;

    // This is the same connection that is used in the wrapped IngestionService
    private final Connection connection;

    private final String postProcessingScript;
    private final boolean manageTransaction;

    public IngestionServiceWithPostProcessing(IngestionService ingestionService, Connection connection, String postProcessingScript) {
        this(ingestionService, connection, postProcessingScript, true);
    }

    public IngestionServiceWithPostProcessing(IngestionService ingestionService, Connection connection, String postProcessingScript, boolean manageTransaction) {
        this.ingestionService = ingestionService;
        this.connection = connection;
        this.postProcessingScript = postProcessingScript;
        this.manageTransaction = manageTransaction;
    }

    @Override
    public void addRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        String batchId = UUID.randomUUID().toString();
        log.info("Using batch id: {}", batchId);

        List<AbstractFireboltRecord> batchIdFireboltRecords = fireboltRecords.stream()
                .map(fireboltRecord -> new BatchIdFireboltRecord(fireboltRecord, batchId))
                .collect(Collectors.toList());

        if (manageTransaction) {
            connection.setAutoCommit(false);
            try {
                executeIngestionAndPostProcessing(batchIdFireboltRecords, batchId);
                connection.commit();
            } catch (SQLException ex) {
                log.error("Error during ingestion with post-processing, rolling back: {}", ex.getMessage());
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        } else {
            // Transaction is managed externally (by TableWriter in exactly-once mode)
            executeIngestionAndPostProcessing(batchIdFireboltRecords, batchId);
        }
    }

    private void executeIngestionAndPostProcessing(List<AbstractFireboltRecord> records, String batchId) throws SQLException {
        ingestionService.addRecords(records);
        try (Statement statement = connection.createStatement()) {
            log.info("Executing the post processing script");
            statement.execute(processScript(postProcessingScript, batchId));
        }
    }

    @Override
    public void close() {
        if (ingestionService != null) {
            try {
                ingestionService.close();
            } catch (Exception e) {
                log.error("Failed to close the ingestion service");
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Failed to close connection");
            }
        }
    }

    /**
     * Replaces ${firebolt_param.batch_id} placeholders in the script with the generated batch id.
     */
    String processScript(String script, String batchId) {
        if (script == null || script.isBlank()) {
            return script;
        }
        java.util.Map<String, String> values = java.util.Map.of(FIREBOLT_BATCH_ID_KEY, batchId);
        return StringSubstitutor.replace(script, values, "${", "}");
    }

    private class BatchIdFireboltRecord implements AbstractFireboltRecord {

        private AbstractFireboltRecord fireboltRecord;
        private String batchId;

        public BatchIdFireboltRecord(AbstractFireboltRecord fireboltRecord, String batchId) {
            this.fireboltRecord = fireboltRecord;
            this.batchId = batchId;
        }

        @Override
        public String getTableName() {
            return fireboltRecord.getTableName();
        }

        @Override
        public String getTopic() {
            return fireboltRecord.getTopic();
        }

        @Override
        public int getPartition() {
            return fireboltRecord.getPartition();
        }

        @Override
        public long getOffset() {
            return fireboltRecord.getOffset();
        }

        @Override
        public long getTimestamp() {
            return fireboltRecord.getTimestamp();
        }

        @Override
        public boolean hasValueSchema() {
            return fireboltRecord.hasValueSchema();
        }

        @Override
        public Set<String> getColumnNames() {
            Set<String> columnNames = new HashSet<>(fireboltRecord.getColumnNames());
            columnNames.add(BATCH_ID_COLUMN_NAME);
            return columnNames;
        }

        @Override
        public Set<String> getColumnNamesWithNullValues() {
            return fireboltRecord.getColumnNamesWithNullValues();
        }

        @Override
        public KafkaMessageColumnValue getColumnValue(String columnName) {
            if (BATCH_ID_COLUMN_NAME.equals(columnName)) {
                return SchemalessKafkaMessageColumnValue.builder().value(batchId).build();
            }

            return fireboltRecord.getColumnValue(columnName);
        }

        @Override
        public SinkRecord getSinkRecord() {
            return fireboltRecord.getSinkRecord();
        }
    }
}
