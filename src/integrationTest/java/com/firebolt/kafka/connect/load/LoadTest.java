package com.firebolt.kafka.connect.load;

import com.firebolt.kafka.connect.clients.ConfluentResourceClient;
import com.firebolt.kafka.connect.load.messagegenerator.LoadTestRecordMessageGenerator;
import com.firebolt.kafka.connect.load.messagegenerator.MessageGenerator;
import com.firebolt.kafka.connect.load.publisher.AvroSchemaRegistryKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.JsonSchemaRegistryKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.JsonSchemalessKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.KafkaMessagePublisher;
import com.firebolt.kafka.connect.load.verifier.LoadTestRecordFireboltTableVerifier;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadTest {

    private static final Set<String> STAGING_APIS = Set.of("id.staging.firebolt.io", "api.staging.firebolt.io");
    private static final long ONE_MEGA_BYTE = 1024 * 1024;
    
    public static void main(String[] args) throws Exception {
        ConfluentCloudSettings confluentCloudSettings = confluentCloudSettings();
        FireboltSettings fireboltSettings = fireboltSettings();

        // Get message sizes from system property or use default
        String messageSizesStr = System.getProperty("loadtest.message.sizes", "100,500,1000,5000,10000");
        String[] messageSizes = messageSizesStr.split(",");
        
        // Get message count from system property or use default
        int messageCount = Integer.parseInt(System.getProperty("loadtest.message.count", "1000000"));
        
        log.info("Running load test with message sizes: {} and message count: {}", messageSizesStr, messageCount);

        // Get table schema from system property or use default
        String tableSchema = System.getProperty("loadtest.table.schema", "8-column");
        // Get ingestion type from system property or use default
        String ingestionType = System.getProperty("loadtest.ingestion.type", "sql");

        // message type and schema presence
        MessageType messageType = MessageType.fromString(System.getProperty("loadtest.message.type", "json"));
        boolean hasSchema = Boolean.parseBoolean(System.getProperty("loadtest.has.schema", "true"));
        log.info("Load test config -> messageType: {}, hasSchema: {}", messageType.getValue(), hasSchema);

        // Tunables for consumer fetch and poll behavior
        int minFetchMegabytes = Integer.parseInt(System.getProperty("loadtest.fetch.min.megabytes", "20"));
        int maxWaitTimeMs = Integer.parseInt(System.getProperty("loadtest.fetch.max.wait.ms", "2000"));
        int maxPollRecords = Integer.parseInt(System.getProperty("loadtest.max.poll.records", "10000"));
        
        String tableDefinitionFilePath;
        String schemaDefinitionPath = null;

        switch (tableSchema) {
            case "8-column":
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-8-column-table-schema.txt";
                if (hasSchema) {
                    schemaDefinitionPath = messageType == MessageType.JSON ? "src/integrationTest/resources/load/json-schema-8-column-registry.txt"
                            : "src/integrationTest/resources/load/avro-schema-8-column-registry.txt";
                }
                break;
            case "80-column":
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-80-column-table-schema.txt";
                if (hasSchema) {
                    schemaDefinitionPath = messageType == MessageType.JSON ? "src/integrationTest/resources/load/json-schema-80-column-registry.txt"
                            : "src/integrationTest/resources/load/avro-schema-80-column-registry.txt";
                }
                break;
            case "400-column":
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-400-column-table-schema.txt";
                if (hasSchema) {
                    schemaDefinitionPath = messageType == MessageType.JSON ? "src/integrationTest/resources/load/json-schema-400-column-registry.txt"
                            : "src/integrationTest/resources/load/avro-schema-400-column-registry.txt";
                }
                break;
            case "1000-column":
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-1000-column-table-schema.txt";
                if (hasSchema) {
                    schemaDefinitionPath = messageType == MessageType.JSON ? "src/integrationTest/resources/load/json-schema-1000-column-registry.txt"
                            : "src/integrationTest/resources/load/avro-schema-1000-column-registry.txt";
                }
                break;

            case "8-column-with-default-timestamp":
            default:
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-8-column-with-default-timestamp-table-schema.txt";
                if (hasSchema) {
                    schemaDefinitionPath = messageType == MessageType.JSON ? "src/integrationTest/resources/load/json-schema-8-column-registry.txt"
                            : "src/integrationTest/resources/load/avro-schema-8-column-registry.txt";
                }
                break;
        }

        log.info("Using table schema: {} with files: {} and {} (messageType: {})", tableSchema, tableDefinitionFilePath,
                schemaDefinitionPath, messageType.getValue());

        // Run test scenarios for each message size
        for (String messageSizeStr : messageSizes) {
            int messageSize = Integer.parseInt(messageSizeStr.trim());

            LoadTestRecordFireboltTableVerifier loadTestRecordFireboltTableVerifier = new LoadTestRecordFireboltTableVerifier();
            KafkaMessagePublisher messagePublisher = createMessagePublisher(messageSize, loadTestRecordFireboltTableVerifier, messageCount, messageType, hasSchema, schemaDefinitionPath, confluentCloudSettings);

            Map<String,String> connectorPropertiesOverride = new HashMap<>();

            // Apply explicit overrides from inputs
            long minFetchBytes = (long) minFetchMegabytes * 1024 * 1024; // convert MB to bytes
            connectorPropertiesOverride.put("consumer.override.fetch.min.bytes", String.valueOf(minFetchBytes));
            connectorPropertiesOverride.put("consumer.override.fetch.max.bytes", String.valueOf(minFetchBytes + ONE_MEGA_BYTE));
            connectorPropertiesOverride.put("consumer.override.fetch.max.wait.ms", String.valueOf(maxWaitTimeMs));
            connectorPropertiesOverride.put("consumer.override.max.poll.records", String.valueOf(maxPollRecords));
            // since we only have one partition we can just set it to hte
            connectorPropertiesOverride.put("consumer.override.max.partition.fetch.bytes", String.valueOf(minFetchBytes + ONE_MEGA_BYTE));

            // If binary ingestion is selected, override connector ingestion.type
            if ("binary".equalsIgnoreCase(ingestionType)) {
                connectorPropertiesOverride.put("ingestion.type", "binary");
            }

            TestScenario testScenario = TestScenario.builder()
                    .averageMessageSizeInBytes(messageSize)
                    .nrOfKafkaMessageToProduce(messageCount)
                    .connectorName("load-test-connector-" + messageSize)
                    .topicName("load-test-connector-" + messageSize)
                    .tableName("load-test-connector-" + messageSize)
                    .fireboltIngestionWaitDuration(Duration.ofMinutes(60))
                    .tableSchemaDefinitionFilePath(tableDefinitionFilePath)
                    .schemaDefinitionPath(schemaDefinitionPath)
                    .messageType(messageType)
                    .staticOutboundHostnames(STAGING_APIS)
                    .confluentCloudSettings(confluentCloudSettings)
                    .fireboltSettings(fireboltSettings)
                    .connectorConfiguration(connectorPropertiesOverride)
                    .deleteConnector(true)
                    .deleteTable(true)
                    .loadTestKafkaMessagePublisher(messagePublisher)
                    .fireboltTableRecordVerifier(loadTestRecordFireboltTableVerifier)
                    .build();

            log.info("Running test scenario for message size: {} bytes", messageSize);
            LoadTestRunner loadTestRunner = new LoadTestRunner(testScenario);
            try {
                LoadTestRunResult result = loadTestRunner.run();
                log.info("For test scenario {} we have the following results {}", testScenario, result);
            } catch (Exception e) {
                log.error("Failed to complete the run", e);
                throw e;
            }
        }

        printActiveThreads();
    }

    private static KafkaMessagePublisher createMessagePublisher(int messageSize, LoadTestRecordFireboltTableVerifier loadTestRecordFireboltTableVerifier, int messageCount, MessageType messageType, boolean hasSchema, String schemaDefinitionPath, ConfluentCloudSettings confluentCloudSettings) throws IOException {
        try (ConfluentResourceClient confluentResourceClient = new ConfluentResourceClient(confluentCloudSettings.getCloudResourceApiKey(), confluentCloudSettings.getCloudResourceApiSecret())) {
            String schemaRegistryUrl = confluentResourceClient.getSchemaRegistryUrl(confluentCloudSettings.getEnvironmentId());
            String bootstrapServers = confluentResourceClient.getBootstrapServerUrl(confluentCloudSettings().getClusterId(), confluentCloudSettings().getEnvironmentId());

            if (messageType == MessageType.AVRO) {
                if (!hasSchema) {
                    throw new IllegalArgumentException("Avro message type requires hasSchema=true");
                }
                Path basePath = Paths.get("src/integrationTest/resources").toAbsolutePath().normalize();
                Path resolved = basePath.resolve(schemaDefinitionPath).normalize();
                if (!resolved.startsWith(basePath)) {
                    throw new IllegalArgumentException("Invalid schema path: " + schemaDefinitionPath);
                }
                String avroSchema = new String(java.nio.file.Files.readAllBytes(resolved));
                TestRecordFactory testRecordFactory = new TestRecordFactory(messageSize);
                MessageGenerator<LoadTestRecord> messageGenerator = new LoadTestRecordMessageGenerator(testRecordFactory, loadTestRecordFireboltTableVerifier, messageCount);
                return new AvroSchemaRegistryKafkaMessagePublisher(
                        bootstrapServers, confluentCloudSettings().getKafkaApiKey(), confluentCloudSettings().getKafkaApiSecret(),
                        schemaRegistryUrl, confluentCloudSettings().getSchemaRegistryApiKey(), confluentCloudSettings().getSchemaRegistryApiSecret(),
                        avroSchema, messageGenerator);
            }

            TestRecordFactory testRecordFactory = new TestRecordFactory(messageSize);
            MessageGenerator<LoadTestRecord> messageGenerator = new LoadTestRecordMessageGenerator(testRecordFactory, loadTestRecordFireboltTableVerifier, messageCount);

            if (messageType == MessageType.JSON) {
                return hasSchema ? new JsonSchemaRegistryKafkaMessagePublisher<>(
                        bootstrapServers, confluentCloudSettings().getKafkaApiKey(), confluentCloudSettings().getKafkaApiSecret(),
                        schemaRegistryUrl, confluentCloudSettings().getSchemaRegistryApiKey(), confluentCloudSettings().getSchemaRegistryApiSecret(),
                        messageGenerator) : new JsonSchemalessKafkaMessagePublisher(bootstrapServers, confluentCloudSettings().getKafkaApiKey(), confluentCloudSettings().getKafkaApiSecret(), messageGenerator);
            }

            throw new IllegalArgumentException("Unsupported message type: " + messageType.getValue() + ". Use 'json' or 'avro'.");
        }
    }

    private static FireboltSettings fireboltSettings() {
        return FireboltSettings.builder()
                .jdbcUrl(System.getProperty("firebolt.jdbc.url", "jdbc:firebolt:sink-connector-load-test?account=goprean-us-east&engine=sink_connector_load_test&env=staging"))
                .clientId(System.getProperty("firebolt.client.id"))
                .clientSecret(System.getProperty("firebolt.client.secret"))
                .build();
    }

    /**
     * Debug method to print all active threads and their states.
     * Call this at the end of main method to see which threads are preventing JVM shutdown.
     */
    public static void printActiveThreads() {
        log.info("=== ACTIVE THREADS DEBUG ===");
        ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
        ThreadGroup parentGroup;
        while ((parentGroup = rootGroup.getParent()) != null) {
            rootGroup = rootGroup.getParent();
        }

        Thread[] threads = new Thread[rootGroup.activeCount() * 2];
        int count = rootGroup.enumerate(threads, true);

        log.info("Total active threads: {}", count);
        for (int i = 0; i < count; i++) {
            Thread thread = threads[i];
            if (thread != null) {
                log.info("Thread[{}]: name='{}', state={}, daemon={}, alive={}",
                        i, thread.getName(), thread.getState(), thread.isDaemon(), thread.isAlive());

                // Print stack trace for non-daemon threads that might be preventing shutdown
                if (!thread.isDaemon() && thread.isAlive() && !thread.getName().equals("main")) {
                    log.warn("Non-daemon thread '{}' may be preventing JVM shutdown:", thread.getName());
                    StackTraceElement[] stackTrace = thread.getStackTrace();
                    for (int j = 0; j < Math.min(5, stackTrace.length); j++) {
                        log.warn("  at {}", stackTrace[j]);
                    }
                }
            }
        }
        log.info("=== END ACTIVE THREADS DEBUG ===");
    }

    private static ConfluentCloudSettings confluentCloudSettings() {
        return ConfluentCloudSettings.builder()
                .environmentId(System.getProperty("confluent.environment.id"))
                .clusterId(System.getProperty("confluent.cluster.id"))
                .fireboltConnectorPluginId(System.getProperty("confluent.firebolt.connector.plugin.id"))
                .kafkaApiKey(System.getProperty("confluent.kafka.api.key"))
                .kafkaApiSecret(System.getProperty("confluent.kafka.api.secret"))
                .schemaRegistryApiKey(System.getProperty("confluent.schema.registry.api.key"))
                .schemaRegistryApiSecret(System.getProperty("confluent.schema.registry.api.secret"))
                .cloudResourceApiKey(System.getProperty("confluent.cloud.resource.api.key"))
                .cloudResourceApiSecret(System.getProperty("confluent.cloud.resource.api.secret"))
                .build();
    }

}
