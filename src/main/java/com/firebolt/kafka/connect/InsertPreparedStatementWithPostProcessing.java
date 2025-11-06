package com.firebolt.kafka.connect;


import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.commons.text.StringSubstitutor;

/**
 *  Will use batch inserts with prepared statements. It will also open a transaction and commit the transaction.
 *  It will include a batchId and will run the post-processing script
 */
@Slf4j
public class InsertPreparedStatementWithPostProcessing extends InsertPreparedStatement {

    private static final String BATCH_ID_COLUMN_NAME = "batch_id";
    private static final String FIREBOLT_BATCH_ID_KEY = "firebolt_param.batch_id";

    private String postProcessingScript;

    public InsertPreparedStatementWithPostProcessing(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, boolean errorToleranceAll, String postProcessingScript) {
        super(connection, tableSchema, errorReporter, errorToleranceAll);
        this.postProcessingScript = postProcessingScript;
    }

    @Override
    public void addRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        String batchId = UUID.randomUUID().toString();
        log.info("Using batch id: {}", batchId);

        // amend all the firebolt records with a batch id
        List<AbstractFireboltRecord> batchIdFireboltRecords = fireboltRecords.stream()
                .map(fireboltRecord -> new BatchIdFireboltRecord(fireboltRecord, batchId))
                .collect(Collectors.toList());
        connection.setAutoCommit(false);

        try {
            super.addRecords(batchIdFireboltRecords);

            try (Statement statement = connection.createStatement()) {
                log.info("Executing the post processing script");
                String processedScript = processScript(postProcessingScript, batchId);
                statement.execute(processedScript);
            }

        } catch (SQLException ex) {
            log.error("There was an error so rolling back the transaction: ", ex.getMessage());
            connection.rollback();
            throw ex;
        } finally {
            connection.setAutoCommit(true);
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
