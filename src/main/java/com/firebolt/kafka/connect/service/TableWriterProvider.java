package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.IngestionService;
import com.firebolt.kafka.connect.IngestionServiceProvider;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.TableWriter;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Class responsible for creating the TableWriter with the correct ingestion service based on the configuration from sink config
 */
public class TableWriterProvider {

    private IngestionServiceProvider ingestionServiceProvider;

    public TableWriterProvider() {
        this(new IngestionServiceProvider());
    }

    @VisibleForTesting
    TableWriterProvider(IngestionServiceProvider ingestionServiceProvider) {
        this.ingestionServiceProvider = ingestionServiceProvider;
    }

    public TableWriter get(TableSchema tableSchema, Supplier<Connection> connectionSupplier, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, ErrorReporter errorReporter, boolean errorToleranceAll, SinkConfig sinkConfig) {
        IngestionService ingestionService = ingestionServiceProvider.get(connectionSupplier.get(), tableSchema, errorReporter, errorToleranceAll, sinkConfig);
        return new TableWriter(tableSchema, fireboltMetadataService, topicName, processedPartitionOffsets, ingestionService);
    }
}
