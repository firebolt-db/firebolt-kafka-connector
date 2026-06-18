package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.ingestion.upload.UploadIngestionService;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class IngestionServiceProvider {

    public IngestionService get(Connection connection, String tableName, ErrorReporter errorReporter, SinkConfig sinkConfig) {
        IngestionService ingestionService =
                new UploadIngestionService(connection, errorReporter, sinkConfig.isErrorToleranceAll(), tableName);

        Optional<String> postProcessingScript = sinkConfig.getPostProcessingScript(tableName);
        return postProcessingScript == null || postProcessingScript.isEmpty() ? ingestionService
                : new IngestionServiceWithPostProcessing(ingestionService, connection, postProcessingScript.get());
    }

}
