package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.ingestion.parquet.ParquetIngestionService;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class IngestionServiceProvider {

    public IngestionService get(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, SinkConfig sinkConfig) {
        IngestionService ingestionService =
                new ParquetIngestionService(connection, errorReporter, sinkConfig.isErrorToleranceAll(), tableSchema);

        Optional<String> postProcessingScript = sinkConfig.getPostProcessingScript(tableSchema.getTableName());
        return postProcessingScript == null || postProcessingScript.isEmpty() ? ingestionService
                : new IngestionServiceWithPostProcessing(ingestionService, connection, postProcessingScript.get());
    }

}
