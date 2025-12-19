package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.ingestion.binary.BinaryIngestionService;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.InsertPreparedStatement;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class IngestionServiceProvider {

    public IngestionService get(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, SinkConfig sinkConfig) {
        boolean tolerateAllErrors = sinkConfig.isErrorToleranceAll();
        IngestionService ingestionService = sinkConfig.getIngestionType() == IngestionType.SQL ?
                new InsertPreparedStatement(connection, tableSchema, errorReporter, tolerateAllErrors) : new BinaryIngestionService(connection, errorReporter, tolerateAllErrors, tableSchema);

        Optional<String> postProcessingScript = sinkConfig.getPostProcessingScript(tableSchema.getTableName());
        return postProcessingScript == null || postProcessingScript.isEmpty() ? ingestionService
                : new IngestionServiceWithPostProcessing(ingestionService, connection, postProcessingScript.get());
    }

}
