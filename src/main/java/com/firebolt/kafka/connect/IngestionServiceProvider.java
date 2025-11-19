package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class IngestionServiceProvider {

    public IngestionService get(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, boolean errorToleranceAll, Optional<String> postProcessingScript) {
        IngestionService ingestionService = new InsertPreparedStatement(connection, tableSchema, errorReporter, errorToleranceAll);

        return postProcessingScript == null || postProcessingScript.isEmpty() ? ingestionService
                : new IngestionServiceWithPostProcessing(ingestionService, connection, postProcessingScript.get());
    }

}
