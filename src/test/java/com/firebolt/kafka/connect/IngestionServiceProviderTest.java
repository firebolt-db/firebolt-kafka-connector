package com.firebolt.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.ingestion.upload.UploadIngestionService;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IngestionServiceProviderTest {

    private final IngestionServiceProvider provider = new IngestionServiceProvider();

    @Test
    void returnsTheUploadServiceWhenNoPostProcessingIsConfigured() {
        IngestionService service = provider.get(mock(Connection.class), "t", mock(ErrorReporter.class),
                new SinkConfig(Map.of()));
        assertInstanceOf(UploadIngestionService.class, service);
    }

    @Test
    void wrapsInPostProcessingOnlyForTheConfiguredTable() {
        SinkConfig config = new SinkConfig(Map.of(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG,
                "{\"mappings\":[{\"table\":\"t\",\"script\":\"SELECT 1\"}]}"));

        assertInstanceOf(IngestionServiceWithPostProcessing.class,
                provider.get(mock(Connection.class), "t", mock(ErrorReporter.class), config));
        assertInstanceOf(UploadIngestionService.class,
                provider.get(mock(Connection.class), "other", mock(ErrorReporter.class), config));
    }
}
