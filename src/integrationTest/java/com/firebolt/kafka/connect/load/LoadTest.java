package com.firebolt.kafka.connect.load;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadTest {

    private static final Set<String> STAGING_APIS = Set.of("id.staging.firebolt.io", "api.staging.firebolt.io");

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
        
        // Select schema files based on table schema
        String tableDefinitionFilePath;
        String jsonSchemaDefinitionFilePathPath;
        
        switch (tableSchema) {
            case "8-column":
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-8-column-table-schema.txt";
                jsonSchemaDefinitionFilePathPath = "src/integrationTest/resources/load/json-schema-8-column-registry.txt";
                break;
            case "80-column":
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-80-column-table-schema.txt";
                jsonSchemaDefinitionFilePathPath = "src/integrationTest/resources/load/json-schema-80-column-registry.txt";
                break;
            case "8-column-with-default-timestamp":
            default:
                tableDefinitionFilePath = "src/integrationTest/resources/load/firebolt-8-column-with-default-timestamp-table-schema.txt";
                jsonSchemaDefinitionFilePathPath = "src/integrationTest/resources/load/json-schema-8-column-registry.txt";
                break;
        }
        
        log.info("Using table schema: {} with files: {} and {}", tableSchema, tableDefinitionFilePath, jsonSchemaDefinitionFilePathPath);

        // Run test scenarios for each message size
        for (String messageSizeStr : messageSizes) {
            int messageSize = Integer.parseInt(messageSizeStr.trim());
            
            TestScenario testScenario = TestScenario.builder()
                    .averageMessageSizeInBytes(messageSize)
                    .nrOfKafkaMessageToProduce(messageCount)
                    .connectorName("load-test-connector-" + messageSize)
                    .topicName("load-test-connector-" + messageSize)
                    .fireboltIngestionWaitDuration(Duration.ofMinutes(60))
                    .tableSchemaDefinitionFilePath(tableDefinitionFilePath)
                    .jsonSchemaRegistryDefinitionFilePath(jsonSchemaDefinitionFilePathPath)
                    .staticOutboundHostnames(STAGING_APIS)
                    .confluentCloudSettings(confluentCloudSettings)
                    .fireboltSettings(fireboltSettings)
                    .deleteConnector(true)
                    .deleteTable(true)
                    .build();

            // Add special configuration for larger message sizes
            if (messageSize >= 10000) {
                testScenario = TestScenario.builder()
                        .averageMessageSizeInBytes(messageSize)
                        .nrOfKafkaMessageToProduce(messageCount)
                        .connectorName("load-test-connector-" + messageSize)
                        .topicName("load-test-connector-" + messageSize)
                        .fireboltIngestionWaitDuration(Duration.ofMinutes(60))
                        .tableSchemaDefinitionFilePath(tableDefinitionFilePath)
                        .jsonSchemaRegistryDefinitionFilePath(jsonSchemaDefinitionFilePathPath)
                        .staticOutboundHostnames(STAGING_APIS)
                        .confluentCloudSettings(confluentCloudSettings)
                        .fireboltSettings(fireboltSettings)
                        .connectorConfiguration(Map.of("consumer.override.max.poll.records", "3000"))
                        .deleteConnector(true)
                        .deleteTable(true)
                        .build();
            }

            log.info("Running test scenario for message size: {} bytes", messageSize);
            LoadTestRunner loadTestRunner = new LoadTestRunner(testScenario);
            LoadTestRunResult result = loadTestRunner.run();
            log.info("For test scenario {} we have the following results {}", testScenario, result);
        }

        printActiveThreads();
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
                .environmentId(System.getProperty("confluent.environment.id", "env-7kddyw"))
                .clusterId(System.getProperty("confluent.cluster.id","lkc-92g0z5"))
                .fireboltConnectorPluginId(System.getProperty("confluent.firebolt.connector.plugin.id"))
                .kafkaApiKey(System.getProperty("confluent.kafka.api.key", "NMBLCRMREIGL6VVZ"))
                .kafkaApiSecret(System.getProperty("confluent.kafka.api.secret","cfltvzQXBJD4DvbS5AvMtXLkieFMC1kyAaNV2ISfLw+YJq0nppRVTMmp2fPD4VKg"))
                .schemaRegistryApiKey(System.getProperty("confluent.schema.registry.api.key","HG2ND4LMLGM7CBS6"))
                .schemaRegistryApiSecret(System.getProperty("confluent.schema.registry.api.secret","cflt1GzYmP8U+gHDSBRLulsTXCn1Qx//RQhfneu9QQrg0vwhW6vJEBV4Erd+roTQ"))
                .cloudResourceApiKey(System.getProperty("confluent.cloud.resource.api.key","NGGLO2BNZPES2T3D"))
                .cloudResourceApiSecret(System.getProperty("confluent.cloud.resource.api.secret","cfltmYZVVpQrInvwMkOv4Ul8hAIz1IyBYd9fxXm92sAu25zhf1fT3c35r3WrRePg"))
                .build();
    }

}
