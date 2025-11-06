package com.firebolt.kafka.connect.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.firebolt.kafka.connect.PostProcessingConfig;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Load test that fetches records from a query and runs in schemaless mode with post-processing script.
 * 
 * Usage:
 * - Set system property "loadtest.query" to provide the SQL query to fetch records
 * - Set system property "loadtest.post.processing.script" to provide post-processing script (optional, defaults to "select 1")
 * - Set system property "loadtest.table.schema" to provide table schema file path (optional)
 */
@Slf4j
public class SchemalessQueryLoadTest {

    private static final Set<String> STAGING_APIS = Set.of("id.staging.firebolt.io", "api.staging.firebolt.io");

    public static void main(String[] args) throws Exception {
        ConfluentCloudSettings confluentCloudSettings = confluentCloudSettings();
        FireboltSettings fireboltSettings = fireboltSettings();

        // Get query from system property
        String query = System.getProperty("loadtest.query");
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("System property 'loadtest.query' must be provided with a SQL query to fetch records");
        }

        // Get post-processing script from system property (defaults to "select 1")
        String postProcessingScript = System.getProperty("loadtest.post.processing.script", "select 1");
        
        // Get table schema file path (optional, for creating the target table)
        String tableSchemaFilePath = System.getProperty("loadtest.table.schema");
        
        // Get connector name
        String connectorName = System.getProperty("loadtest.connector.name", "schemaless-query-load-test");
        
        // Get topic name
        String topicName = System.getProperty("loadtest.topic.name", "schemaless-query-load-test-topic");
        
        log.info("Running schemaless load test with query-based record fetching");
        log.info("Query: {}", query);
        log.info("Post-processing script: {}", postProcessingScript);

        // Prepare post-processing script configuration
        String postProcessingScriptJson = preparePostProcessingScript(topicName, postProcessingScript);

        TestScenario testScenario = TestScenario.builder()
                .connectorName(connectorName)
                .topicName(topicName)
                .schemaless(true) // Enable schemaless mode
                .recordFetchQuery(query) // Query to fetch records
                .postProcessingScript(postProcessingScriptJson) // Post-processing script
                .fireboltIngestionWaitDuration(Duration.ofMinutes(60))
                .tableSchemaDefinitionFilePath(tableSchemaFilePath) // Optional table schema
                .staticOutboundHostnames(STAGING_APIS)
                .confluentCloudSettings(confluentCloudSettings)
                .fireboltSettings(fireboltSettings)
                .deleteConnector(true)
                .deleteTable(true)
                .nrOfKafkaMessageToProduce(0) // Not used when query-based
                .averageMessageSizeInBytes(0) // Not used when query-based
                .build();

        log.info("Running test scenario: {}", testScenario);
        SchemalessQueryLoadTestRunner loadTestRunner = new SchemalessQueryLoadTestRunner(testScenario, query, postProcessingScriptJson);
        LoadTestRunResult result = loadTestRunner.run();
        log.info("Load test completed with results: {}", result);

        printActiveThreads();
    }

    /**
     * Prepares the post-processing script configuration in JSON format.
     * 
     * @param tableName the table name to apply the script to
     * @param script the SQL script to execute (e.g., "select 1")
     * @return JSON string representation of the post-processing configuration
     */
    private static String preparePostProcessingScript(String tableName, String script) {
        PostProcessingConfig postProcessingConfig = new PostProcessingConfig(
                List.of(
                        new PostProcessingConfig.Mapping(tableName, script)
                ));
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(postProcessingConfig);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize post-processing config", e);
            throw new RuntimeException("Failed to serialize post-processing config", e);
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

