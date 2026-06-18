package com.firebolt.kafka.connect;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 *  Use the decorator pattern to wrap the ingestion service with a post-processing script.
 *  First the decorated service will run its logic, then we will run the post-processing script in the same transaction
 */
@Slf4j
public class IngestionServiceWithPostProcessing implements IngestionService {

    static final String BATCH_ID_COLUMN_NAME = "batch_id";
    private static final String FIREBOLT_BATCH_ID_KEY = "firebolt_param.batch_id";

    private IngestionService ingestionService;

    // This is the same connection that is used in the IngestionService
    private Connection connection;

    private String postProcessingScript;

    public IngestionServiceWithPostProcessing(IngestionService ingestionService, Connection connection, String postProcessingScript) {
        this.ingestionService = ingestionService;
        this.connection = connection;
        this.postProcessingScript = postProcessingScript;
    }

    @Override
    public void addRecords(List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        String batchId = UUID.randomUUID().toString();
        log.info("Using batch id: {}", batchId);

        Map<String, String> literalColumnsWithBatchId = new HashMap<>(literalColumns);
        literalColumnsWithBatchId.put(BATCH_ID_COLUMN_NAME, batchId);

        connection.setAutoCommit(false);

        try {
            ingestionService.addRecords(records, literalColumnsWithBatchId);

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
            try {
                connection.commit();
            } catch (SQLException e) {
                log.error("Failed to commit the transaction. Will rollback");
                connection.rollback();

                // rethrow the original exception
                throw e;
            }
        }

    }

    @Override
    public void close() {
        if (ingestionService!= null) {
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

}
