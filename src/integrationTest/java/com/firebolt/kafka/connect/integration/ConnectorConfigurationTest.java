package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.utils.TestTag;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for connector configurations.
 * 
 * These tests verify that the Firebolt Sink Connector properly validates
 * configuration parameters and rejects invalid configurations with meaningful
 * error messages.
 */
@Slf4j
public class ConnectorConfigurationTest extends BaseIntegrationTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static FireboltClient fireboltClient;

    @BeforeAll
    static void setupClass() throws SQLException {
        // make sure we do have certain tables (since we do verify that when we validate the connector configuration)
        fireboltClient = FireboltClient.createFor(getDatabaseName());

        fireboltClient.createStandardTestTable("table1");
        fireboltClient.createStandardTestTable("table2");
    }

    @AfterAll
    static void tearDownClass() throws SQLException {
        fireboltClient.dropTable("table1");
        fireboltClient.dropTable("table2");
        fireboltClient.close();
    }

    @BeforeEach
    void setupTest(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("invalid-connector-");
    }
    
    @Test
    void testMissingRequiredJdbcUrl() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        // Remove the required JDBC URL
        connectorConfig.remove("jdbc.connection.url");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about missing URL
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("jdbc.connection.url") || 
                   errorMessage.toLowerCase().contains("connection url"), 
                   "Error message should mention missing JDBC URL: " + errorMessage);
    }
    
    @Test
    void testInvalidJdbcUrlFormat() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("jdbc.connection.url", "invalid-url-format");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about invalid URL format
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("jdbc:firebolt") || 
                   errorMessage.toLowerCase().contains("connection url") ||
                   errorMessage.toLowerCase().contains("invalid"),
                   "Error message should mention invalid JDBC URL format: " + errorMessage);
    }

    @Test
    @Tag(TestTag.CLOUD)
    void shouldNotAllowConnectWithoutEngine() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        String jdbcUrl = (String) connectorConfig.get("jdbc.connection.url");

        // remove the engine from the jdbc url and create the connector with this new jdbc url
        String jdbcUrlWithoutEngine = removeEngine(jdbcUrl);
        connectorConfig.put("jdbc.connection.url", jdbcUrlWithoutEngine);

        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);

        // Verify error message contains information about invalid URL format
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.contains("The jdbc url does not have the engine parameter. When connecting to Firebolt Cloud this is mandatory."));
    }

    @Test
    @Tag(TestTag.CLOUD)
    void shouldNotAllowConnectWithoutAccount() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        String jdbcUrl = (String) connectorConfig.get("jdbc.connection.url");

        // remove the engine from the jdbc url and create the connector with this new jdbc url
        String jdbcUrlWithoutAccount = removeAccount(jdbcUrl);
        connectorConfig.put("jdbc.connection.url", jdbcUrlWithoutAccount);

        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);

        // Verify error message contains information about invalid URL format
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.contains("The jdbc url does not have the account parameter. When connecting to Firebolt Cloud this is mandatory."));
    }

    @Test
    @Tag(TestTag.CLOUD)
    void shouldNotAllowConnectWithoutDatabase() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        String jdbcUrl = (String) connectorConfig.get("jdbc.connection.url");

        // remove the engine from the jdbc url and create the connector with this new jdbc url
        String jdbcUrlWithoutDatabase = removeDatabase(jdbcUrl);
        connectorConfig.put("jdbc.connection.url", jdbcUrlWithoutDatabase);

        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);

        // Verify error message contains information about invalid URL format
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.contains("The jdbc url does not have the database parameter. When connecting to Firebolt Cloud this is mandatory."));
    }

    @Test
    void testInvalidDatabaseInJdbcConnectionUrl() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();

        // Use a URL with the same structure as the default but with a non-existing database
        String databaseName = getDatabaseName();

        String invalidUrl = getJdbcConnectionUrl().replaceFirst(databaseName, "non_existing_db");
        connectorConfig.put("jdbc.connection.url", invalidUrl);
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains connection-related information
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("connection") || 
                   errorMessage.toLowerCase().contains("connect") ||
                   errorMessage.toLowerCase().contains("failed"),
                   "Error message should mention connection failure: " + errorMessage);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "topic1:",                              // Missing table name
        "topic1:table1,topic2:",                // Missing table name after comma
        "topic1:table1:topic2:table1",          // Too many colons
        "invalid-mapping-format",               // No colon separator
        ":table1",                              // Missing topic name
        "topic1:table1,,topic2:table2",         // Double comma
        "topic1:table1:,topic2:table2",         // Colon followed by comma
        "topic1table1",                         // No separator
        "topic1::table1"                        // Double colon
    })
    void testInvalidTopicToTableMappingFormat(String invalidMapping) throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("topic.to.table.mapping", invalidMapping);
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about invalid mapping format
        assertNotNull(errorMessage, "Error message should not be null for invalid mapping: " + invalidMapping);
        assertTrue(errorMessage.toLowerCase().contains("topic.to.table.mapping") || 
                   errorMessage.toLowerCase().contains("mapping") ||
                   errorMessage.toLowerCase().contains("format") ||
                   errorMessage.toLowerCase().contains("invalid"),
                   "Error message should mention invalid mapping format for '" + invalidMapping + "': " + errorMessage);
    }
    
    @Test
    void testDuplicateTopicsInMapping() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("topic.to.table.mapping", "topic1:table1,topic1:table2");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about duplicate topics
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("duplicate") || 
                   errorMessage.toLowerCase().contains("topic") ||
                   errorMessage.toLowerCase().contains("mapping"),
                   "Error message should mention duplicate topics: " + errorMessage);
    }
    
    @Test
    void testDuplicateTablesInMapping() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("topic.to.table.mapping", "topic1:table1,topic2:table1");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about duplicate tables
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("duplicate") || 
                   errorMessage.toLowerCase().contains("table") ||
                   errorMessage.toLowerCase().contains("mapping"),
                   "Error message should mention duplicate tables: " + errorMessage);
    }
    
    @Test
    void testEmptyTopicToTableMappingButWithATopicNameThatDoesNotCorrespondToATable() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("topic.to.table.mapping", "");
        connectorConfig.put("topics", "not-a-firebolt-table");

        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about empty mapping
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("topic.to.table.mapping") || 
                   errorMessage.toLowerCase().contains("empty") ||
                   errorMessage.toLowerCase().contains("mapping"),
                   "Error message should mention empty mapping: " + errorMessage);
    }
    
    @Test
    void testMissingConnectorClass() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.remove("connector.class");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about missing connector.class
        // Note: Kafka Connect itself validates this core property and reports it as "no connector type"
        // This is Kafka Connect's terminology for the missing connector.class property
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("connector type") || 
                   errorMessage.toLowerCase().contains("no connector type"),
                   "Error message should mention missing connector.class (reported as 'connector type' by Kafka Connect): " + errorMessage);
    }
    
    @Test
    void testMissingTopicsConfiguration() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.remove("topics");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about missing topics
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("topics") || 
                   errorMessage.toLowerCase().contains("topics.regex"),
                   "Error message should mention missing topics configuration: " + errorMessage);
    }
    
    @Test
    void testInvalidTasksMaxValue() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("tasks.max", "0");  // Invalid: must be > 0
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about invalid tasks.max
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("tasks.max") || 
                   errorMessage.toLowerCase().contains("tasks") ||
                   errorMessage.toLowerCase().contains("greater than 0"),
                   "Error message should mention invalid tasks.max value: " + errorMessage);
    }

    @Test
    void testInvalidExactlyOnceValue() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("exactlyOnce", "unknown");

        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);

        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.contains("Invalid value unknown for configuration exactlyOnce: Expected value to be either true or false"),
                   "Error message should indicate invalid boolean for exactlyOnce: " + errorMessage);
    }
    
    @Test
    void testInvalidConnectorClass() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        connectorConfig.put("connector.class", "com.invalid.NonExistentConnector");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about invalid connector class
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("connector.class") || 
                   errorMessage.toLowerCase().contains("class") ||
                   errorMessage.toLowerCase().contains("not found") ||
                   errorMessage.toLowerCase().contains("invalid"),
                   "Error message should mention invalid connector class: " + errorMessage);
    }

    @Test
    void testInvalidPostProcessingScriptJson() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        // invalid JSON should fail validation
        connectorConfig.put("post.processing.script", "{ invalid json");

        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);

        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("post.processing.script"),
                   "Error message should mention invalid post.processing.script JSON: " + errorMessage);
    }

    @Test
    void testNonExistentTablesInMapping() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        // Use table names that are very unlikely to exist
        connectorConfig.put("topic.to.table.mapping", "topic1:non_existent_table_12345,topic2:another_missing_table_67890");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about non-existent tables
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("table") && 
                   (errorMessage.toLowerCase().contains("not exist") || 
                    errorMessage.toLowerCase().contains("does not exist") ||
                    errorMessage.toLowerCase().contains("do not exist")),
                   "Error message should mention non-existent tables: " + errorMessage);
    }
    
    @Test
    void testSpecificNonExistentTableValidation() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        // Reference a table that definitely doesn't exist (using a very specific name)
        connectorConfig.put("topic.to.table.mapping", "test-topic:definitely_does_not_exist_table_xyz123");
        
        // Attempt to create connector and expect failure due to table validation
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message specifically mentions the table existence issue
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("table") && 
                   (errorMessage.toLowerCase().contains("not exist") || 
                    errorMessage.toLowerCase().contains("does not exist") ||
                    errorMessage.toLowerCase().contains("do not exist") ||
                    errorMessage.toLowerCase().contains("missing")),
                   "Error message should specifically mention non-existent table: " + errorMessage);
        
        // Should mention the specific table name that doesn't exist
        assertTrue(errorMessage.toLowerCase().contains("definitely_does_not_exist_table_xyz123"),
                   "Error message should mention the specific non-existent table name: " + errorMessage);
    }
    
    @Test
    void testMixedExistingAndNonExistentTablesInMapping() throws IOException {
        Map<String, Object> connectorConfig = createBaseConnectorConfig();
        // Map to existing tables (table1, table2) and one non-existent table
        connectorConfig.put("topic.to.table.mapping", "topic1:table1,topic2:table2,topic3:non_existent_table_xyz");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message specifically mentions the non-existent table
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.toLowerCase().contains("table") && 
                   (errorMessage.toLowerCase().contains("not exist") || 
                    errorMessage.toLowerCase().contains("does not exist") ||
                    errorMessage.toLowerCase().contains("do not exist") ||
                    errorMessage.toLowerCase().contains("missing")),
                   "Error message should mention non-existent table: " + errorMessage);
        
        // Should mention the specific non-existent table name
        assertTrue(errorMessage.toLowerCase().contains("non_existent_table_xyz"),
                   "Error message should mention the specific non-existent table name: " + errorMessage);
        
        // Should NOT fail because of the existing tables
        assertTrue(!(errorMessage.toLowerCase().contains("table1") && 
                    (errorMessage.toLowerCase().contains("not exist") || 
                     errorMessage.toLowerCase().contains("does not exist"))),
                   "Error should NOT mention existing table1 as missing: " + errorMessage);
        assertTrue(!(errorMessage.toLowerCase().contains("table2") && 
                    (errorMessage.toLowerCase().contains("not exist") || 
                     errorMessage.toLowerCase().contains("does not exist"))),
                   "Error should NOT mention existing table2 as missing: " + errorMessage);
    }
    
    @Test
    void testMultipleValidationErrors() throws IOException {
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("name", testConnectorName);
        // Missing connector.class
        // Missing jdbc.connection.url
        // Missing topic.to.table.mapping
        // Invalid sink.connector.type
        connectorConfig.put("sink.connector.type", "invalid-type");
        connectorConfig.put("table.auto.create", "invalid-boolean");
        
        // Attempt to create connector and expect failure
        String errorMessage = createConnectorExpectingFailure(testConnectorName, connectorConfig);
        
        // Verify error message contains information about multiple validation errors
        assertNotNull(errorMessage, "Error message should not be null");
        assertTrue(errorMessage.length() > 50, 
                   "Error message should be substantial for multiple errors: " + errorMessage);
    }
    
    private Map<String, Object> createBaseConnectorConfig() {
        Map<String, Object> config = new HashMap<>();
        
        // === KAFKA CONNECT CORE PROPERTIES ===
        config.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");  // Which connector implementation to use
        config.put("tasks.max", "1");                    // Number of tasks
        config.put("topics", "test-topic");              // Required by Kafka Connect
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        
        // === FIREBOLT CONNECTOR SPECIFIC PROPERTIES ===
        config.put("jdbc.connection.url", getJdbcConnectionUrl());
        config.put("topic.to.table.mapping", "test-topic:table1");
        
        // Add client credentials if system properties are set
        String clientId = getClientId();
        if (clientId != null) {
            config.put("firebolt.clientId", "${file:/etc/kafka-connect/secrets/secrets.properties:clientId}");
        }
        
        String clientSecret = getClientSecret();
        if (clientSecret != null) {
            config.put("firebolt.clientSecret", "${file:/etc/kafka-connect/secrets/secrets.properties:clientSecret}");
        }
        
        return config;
    }
    
    /**
     * Attempts to create a connector expecting it to fail, and returns the error message.
     * 
     * @param connectorName the name of the connector
     * @param config the connector configuration
     * @return the error message from the failed connector creation
     * @throws IOException if the HTTP request fails
     */
    private String createConnectorExpectingFailure(String connectorName, Map<String, Object> config) throws IOException {
        log.info("Creating connector expecting failure: {}", connectorName);
        
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("name", connectorName);
        connectorConfig.put("config", config);
        
        String json = objectMapper.writeValueAsString(connectorConfig);
        
        RequestBody body = RequestBody.create(json, MediaType.parse("application/json"));
        Request request = new Request.Builder()
                .url(KAFKA_CONNECT_HOST + "/connectors")
                .post(body)
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body().string();
            
            if (response.isSuccessful()) {
                // If creation succeeded, clean up the connector and fail the test
                try {
                    deleteConnector(connectorName);
                } catch (Exception e) {
                    log.warn("Failed to cleanup unexpectedly created connector: {}", e.getMessage());
                }
                fail("Expected connector creation to fail, but it succeeded. Response: " + responseBody);
                return null; // Never reached
            } else {
                log.info("Connector creation failed as expected. Status: {}, Response: {}", 
                        response.code(), responseBody);
                return responseBody;
            }
        }
    }

    /**
     * Attempts to create a connector expecting it to succeed with a specific connector name.
     * 
     * @param connectorName the name of the connector
     * @param config the connector configuration
     * @return true if the connector was created successfully, false otherwise
     * @throws IOException if the HTTP request fails
     */
    private boolean createConnectorExpectingSuccessWithName(String connectorName, Map<String, Object> config) throws IOException {
        log.info("Creating connector expecting success: {}", connectorName);
        
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("name", connectorName);
        connectorConfig.put("config", config);
        
        String json = objectMapper.writeValueAsString(connectorConfig);
        
        RequestBody body = RequestBody.create(json, MediaType.parse("application/json"));
        Request request = new Request.Builder()
                .url(KAFKA_CONNECT_HOST + "/connectors")
                .post(body)
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body().string();
            
            if (response.isSuccessful()) {
                log.info("Connector creation succeeded as expected. Status: {}, Response: {}", 
                        response.code(), responseBody);
                return true;
            } else {
                log.error("Connector creation failed unexpectedly. Status: {}, Response: {}", 
                        response.code(), responseBody);
                return false;
            }
        }
    }
    
    @Nested
    class ValidConfigurations {
        
        private String successfulConnectorName;
        
        @BeforeEach
        void setupSuccessTest(TestInfo testInfo) {
            // Generate unique connector name for successful tests
            String testId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
            successfulConnectorName = "valid-connector-" + testId;
        }
        
        @AfterEach
        void cleanupSuccessTest() {
            if (successfulConnectorName != null) {
                try {
                    deleteConnector(successfulConnectorName);
                    log.info("Cleaned up connector: {}", successfulConnectorName);
                } catch (Exception e) {
                    log.warn("Failed to cleanup connector {}: {}", successfulConnectorName, e.getMessage());
                }
            }
        }
        
        @Test
        void testExistingTableInMapping() throws IOException {
            Map<String, Object> connectorConfig = createBaseConnectorConfig();
            // Reference a table that exists (created in @BeforeAll)
            connectorConfig.put("topic.to.table.mapping", "test-topic:table1");
            
            // Attempt to create connector - this should succeed since table exists
            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, connectorConfig);
            
            // Verify that the connector was created successfully
            assertTrue(success, "Connector creation should succeed when table exists");
        }


        @Test
        void testCanMapMultipleTopicsToTables() throws IOException {
            Map<String, Object> connectorConfig = createBaseConnectorConfig();
            // Reference tables that exist (created in @BeforeAll)
            connectorConfig.put("topic.to.table.mapping", "topic1:table1,topic2:table2");

            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, connectorConfig);

            // Verify that the connector was created successfully
            assertTrue(success, "Connector creation should succeed when all tables in mapping exists");
        }

        @Test
        void testOptionalTopicToTableMapping() throws IOException {
            Map<String, Object> connectorConfig = createBaseConnectorConfig();
            connectorConfig.remove("topic.to.table.mapping");
            // but the topic should be a valid table name
            connectorConfig.put("topics", "table1");

            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, connectorConfig);
            
            // Verify that the connector was created successfully
            assertTrue(success, "Connector creation should succeed without explicit topic.to.table.mapping (may use default topic-to-table mapping)");
        }
        
        @Test
        void testMinimalValidConfiguration() throws IOException {
            Map<String, Object> config = new HashMap<>();
            
            // === KAFKA CONNECT CORE PROPERTIES (REQUIRED) ===
            config.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");
            config.put("tasks.max", "1");
            config.put("topics", "test-topic");
            config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
            config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
            
            // === FIREBOLT CONNECTOR MINIMAL PROPERTIES ===
            config.put("jdbc.connection.url", getJdbcConnectionUrl());
            config.put("topic.to.table.mapping", "test-topic:table1");
            
            // Add client credentials if system properties are set
            String clientId = getClientId();
            if (clientId != null) {
                config.put("firebolt.clientId", "${file:/etc/kafka-connect/secrets/secrets.properties:clientId}");
            }

            String clientSecret = getClientSecret();
            if (clientSecret != null) {
                config.put("firebolt.clientSecret", "${file:/etc/kafka-connect/secrets/secrets.properties:clientSecret}");
            }
            
            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, config);
            
            // Verify that the connector was created successfully
            assertTrue(success, "Connector creation should succeed with minimal configuration (using defaults for optional properties)");
        }

        @Test
        void testExactlyOnceTrueStartsConnector() throws IOException {
            Map<String, Object> config = createBaseConnectorConfig();
            config.put("exactlyOnce", "true");

            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, config);

            assertTrue(success, "Connector creation should succeed when exactlyOnce is true");
        }

        @Test
        void testExactlyOnceOmittedDefaultsToFalseAndStartsConnector() throws IOException {
            Map<String, Object> config = createBaseConnectorConfig();
            // do not set exactlyOnce, should default to false

            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, config);

            assertTrue(success, "Connector creation should succeed when exactlyOnce is omitted (defaults to false)");
        }

        @Test
        void testValidPostProcessingScriptJsonParsesAndStartsConnector() throws IOException {
            Map<String, Object> config = createBaseConnectorConfig();
            // valid JSON referencing an existing table 'table1'
            String json = "{\"mappings\":[{\"table\":\"table1\",\"script\":\"UPDATE \\\"table1\\\" SET processed = true\"}]}";
            config.put("post.processing.script", json);

            boolean success = createConnectorExpectingSuccessWithName(successfulConnectorName, config);

            assertTrue(success, "Connector creation should succeed with valid post.processing.script JSON");
        }
    }

    /**
     * Removes the jdbc url from the cloud connection jdbc url. We can assume that the engine will be passed in, so it exists
     */
    private static String removeEngine(String jdbc) {
        return removeJdbcProperty(jdbc, "engine");
    }

    /**
     * Removes the jdbc url from the cloud connection jdbc url. We can assume that the engine will be passed in, so it exists
     */
    private static String removeAccount(String jdbc) {
       return removeJdbcProperty(jdbc, "account");
    }

    private static String removeDatabase(String jdbc) {
        String[] jdbcParts = jdbc.split("\\?");

        jdbcParts[0] = "jdbc:firebolt:";
        return Arrays.stream(jdbcParts).collect(Collectors.joining("?"));
    }

    /**
     * Removes a property from the jdbc url
     */
    private static String removeJdbcProperty(String jdbc, String propertyName) {
        String[] jdbcParts = jdbc.split("\\?");

        String paramString = jdbcParts[1];
        String[] params = paramString.split("&");

        String paramStringWithoutSpecifiedProperty = Arrays.stream(params)
                .filter(param -> !param.toLowerCase().trim().split("=")[0].trim().equals(propertyName))
                .collect(Collectors.joining("&"));

        // use this new part on jdbcParts
        jdbcParts[1] = paramStringWithoutSpecifiedProperty;
        return Arrays.stream(jdbcParts).collect(Collectors.joining("?"));
    }

}