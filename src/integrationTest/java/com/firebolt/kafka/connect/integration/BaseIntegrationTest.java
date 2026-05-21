package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.clients.KafkaConnectClient;
import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import com.firebolt.kafka.connect.utils.JdbcConnectionParser;
import com.firebolt.kafka.connect.utils.ServiceHealthExtension;
import com.firebolt.kafka.connect.utils.TestSetupExtension;
import com.firebolt.kafka.connect.utils.TopicOptions;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({ServiceHealthExtension.class, TestSetupExtension.class})
@Slf4j
public abstract class BaseIntegrationTest {
    
    protected static final String KAFKA_CONNECT_HOST = System.getenv().getOrDefault("KAFKA_CONNECT_URL", "http://localhost:8083");
    protected static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);

    protected static final String DEFAULT_DATABASE_NAME = "integration_test_db";
    protected static final Boolean INCLUDE_NULL_SERIALIZED_VALUES = true;
    protected static final Boolean DO_NOT_INCLUDE_NULL_SERIALIZED_VALUES = false;

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();

    protected String kafkaConnectVersion;
    protected String testName;
    protected String testConnectorName;

    protected FireboltClient fireboltDefaultDbClient;
    protected KafkaConnectClient kafkaConnectClient;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        this.testName = testInfo.getDisplayName();
        this.kafkaConnectVersion = System.getProperty("kafka.connect.version", "unknown");
        
        try {
            this.fireboltDefaultDbClient = FireboltClient.createFor(getDatabaseName());
            this.kafkaConnectClient = new KafkaConnectClient(KAFKA_CONNECT_HOST, httpClient, objectMapper);
        } catch (SQLException e) {
            log.error("Failed to create FireboltClient: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Firebolt connection", e);
        }
        
        log.info("Starting test: {} with Kafka Connect version: {}", testName, kafkaConnectVersion);
    }
    
    @AfterEach
    protected void tearDown() {
        // Close Firebolt client if it was created
        if (fireboltDefaultDbClient != null) {
            try {
                log.debug("Closing Firebolt client for default database for test: {}", testName);
                fireboltDefaultDbClient.close();
            } catch (SQLException e) {
                log.warn("Failed to close Firebolt client: {}", e.getMessage());
            }
            fireboltDefaultDbClient = null;
        }
    }

    protected void createTable(Supplier<String> tableSchemaSupplier, String tableName) throws SQLException {
        log.info("Creating Firebolt test table: {}", tableName);
        String createTableSql = String.format(tableSchemaSupplier.get(), tableName);
        fireboltDefaultDbClient.executeUpdate(createTableSql);
        log.info("Created Firebolt test table with schema");
    }

    /**
     * Creates a Kafka topic with the specified name and options.
     *
     * @param topicName the name of the topic to create
     * @param options the topic configuration options (partitions, replication factor)
     * @throws RuntimeException if topic creation fails (except when topic already exists)
     */
    protected void createKafkaTopic(String topicName, TopicOptions options) {
        log.info("Creating Kafka topic '{}' with options: {}", topicName, options);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topicName, options.getPartitions(), options.getReplicationFactor())
                    .configs(Map.of("max.message.bytes", String.valueOf(50 * 1024 * 1024))); // allow 50 MB messages
            CreateTopicsResult result = adminClient.createTopics(java.util.Collections.singletonList(newTopic));

            // Wait for the topic creation to complete
            result.all().get(60, TimeUnit.SECONDS);

            log.info("Successfully created Kafka topic: {}", topicName);
            // Ensure topic is fully discoverable/ready across the cluster
            waitForTopicReady(topicName, Duration.ofSeconds(60));

        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.info("Topic '{}' already exists, skipping creation", topicName);
            } else {
                log.error("Failed to create topic '{}': {}", topicName, e.getMessage());
                throw new RuntimeException("Failed to create Kafka topic: " + topicName, e);
            }
        } catch (InterruptedException | TimeoutException e) {
            log.error("Timeout or interruption while creating topic '{}': {}", topicName, e.getMessage());
            throw new RuntimeException("Failed to create Kafka topic: " + topicName, e);
        }
    }

    /**
     * Creates a Kafka topic with the specified name using default options
     * (1 partition, replication factor 1).
     *
     * @param topicName the name of the topic to create
     * @throws RuntimeException if topic creation fails (except when topic already exists)
     */
    protected void createKafkaTopic(String topicName) {
        createKafkaTopic(topicName, TopicOptions.defaults());
    }

    protected void waitForTopicReady(String topicName, Duration timeout) {
        log.info("Waiting for topic '{}' to be ready...", topicName);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            await()
                .atMost(timeout)
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> {
                    try {
                        TopicDescription desc = adminClient
                                .describeTopics(java.util.Collections.singletonList(topicName))
                                .values()
                                .get(topicName)
                                .get(10, TimeUnit.SECONDS);
                        boolean ready = desc != null &&
                                desc.partitions() != null &&
                                !desc.partitions().isEmpty() &&
                                desc.partitions().stream().allMatch(p -> p.leader() != null);
                        if (!ready) {
                            log.debug("Topic '{}' not ready yet. Description: {}", topicName, desc);
                        }
                        return ready;
                    } catch (Exception e) {
                        log.debug("Topic '{}' not ready yet: {}", topicName, e.getMessage());
                        return false;
                    }
                });
            log.info("Topic '{}' is ready", topicName);
        }
    }
    
    /**
     * Safely drops a Firebolt table to ensure clean state.
     * This method is useful for test setup to avoid table state conflicts
     * between different test runs.
     * 
     * @param tableName the name of the table to drop
     */
    protected void safelyDropTable(String tableName) {
        log.info("Ensuring clean state by dropping existing table: {}", tableName);
        try {
            fireboltDefaultDbClient.dropTable(tableName);
            log.info("Dropped existing table: {}", tableName);
        } catch (Exception e) {
            log.debug("Table {} did not exist or couldn't be dropped: {}", tableName, e.getMessage());
        }
    }

    protected Map<String, Object> createBasicConnectorDefinition(String topics, String topicToTableMappings) {
        Map<String, Object> connectorConfig = new HashMap<>();

        // Kafka Connect core properties
        connectorConfig.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");
        connectorConfig.put("tasks.max", "1");
        connectorConfig.put("topics", topics);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");

        // JSON Schema converter configuration
        connectorConfig.put("value.converter.json.write.dates.iso8601", "true");

        // Error handling configuration
        connectorConfig.put("errors.tolerance", "all"); //to be able to test all error scenarios

        // Firebolt connector specific properties
        connectorConfig.put("jdbc.connection.url", getJdbcConnectionUrl());
        connectorConfig.put("topic.to.table.mapping", topicToTableMappings);

        // Add client credentials if system properties are set
        String clientId = getClientId();
        if (clientId != null) {
            connectorConfig.put("firebolt.clientId", "${file:/etc/kafka-connect/secrets/secrets.properties:clientId}");
        }

        String clientSecret = getClientSecret();
        if (clientSecret != null) {
            connectorConfig.put("firebolt.clientSecret", "${file:/etc/kafka-connect/secrets/secrets.properties:clientSecret}");
        }

        return connectorConfig;
    }

    protected Properties createBasicProducerProperties(boolean includeNulls) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // configuration for large messages
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 48000000);
        props.put("buffer.memory", "48000000");

        // Configure null handling behavior
        props.put("json.oneof.for.nullables", includeNulls);

        if (!includeNulls) {
            // Omit null fields entirely from JSON output
            props.put("json.default.property.inclusion", "NON_NULL");
        } else {
            // Include null fields in JSON output as "field": null
            props.put("json.default.property.inclusion", "ALWAYS");
        }

        props.put("json.write.dates.iso8601", true);
        props.put("json.indent.output", false);

        return props;
    }

    protected void createConnectorAndWaitForItToStart(String connectorName, String topicToTableMappings, Map<String, Object> connectorConfig) throws IOException {
        // Create the connector
        createConnector(connectorName, connectorConfig);
        kafkaConnectClient.waitForConnectorRunning(connectorName, DEFAULT_TIMEOUT);

        log.info("✅ Connector '{}' registered and running with topic-to-table mapping: {}",
                connectorName, topicToTableMappings);
    }

    /**
     * Safely deletes a Kafka topic to ensure clean state.
     * This method is useful for test cleanup to avoid topic state conflicts
     * between different test runs.
     * 
     * @param topicName the name of the topic to delete
     */
    protected void safelyDeleteKafkaTopic(String topicName) {
        try {
            deleteKafkaTopic(topicName);
        } catch (Exception e) {
            log.warn("Failed to delete Kafka topic {}: {}", topicName, e.getMessage());
        }
    }
    
    /**
     * Deletes a Kafka topic using the AdminClient.
     */
    private void deleteKafkaTopic(String topicName) throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Deleting Kafka topic: {}", topicName);
        
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DeleteTopicsResult result = adminClient.deleteTopics(java.util.Collections.singletonList(topicName));
            result.all().get(60, TimeUnit.SECONDS);
            log.info("Successfully deleted Kafka topic: {}", topicName);
        }
    }
    
    protected String createConnector(String connectorName, Map<String, Object> config) throws IOException {
        log.info("Creating connector: {}", connectorName);
        
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
                log.info("Connector created successfully: {}", connectorName);
                return responseBody;
            } else {
                log.error("Failed to create connector. Response: {}", responseBody);
                throw new RuntimeException("Failed to create connector: " + responseBody);
            }
        }
    }
    
    protected void deleteConnector(String connectorName) throws IOException {
        log.info("Deleting connector: {}", connectorName);
        
        Request request = new Request.Builder()
                .url(KAFKA_CONNECT_HOST + "/connectors/" + connectorName)
                .delete()
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                log.info("Connector deleted successfully: {}", connectorName);
            } else {
                log.warn("Failed to delete connector: {}", connectorName);
            }
        }
    }
    
    /**
     * Safely deletes a Kafka Connect connector with proper error handling.
     * This method is useful for test cleanup to remove connectors created during testing.
     * 
     * @param connectorName the name of the connector to delete
     */
    protected void safelyDeleteConnector(String connectorName) {
        if (connectorName != null) {
            try {
                deleteConnector(connectorName);
                log.info("Deleted connector: {}", connectorName);
            } catch (Exception e) {
                log.warn("Failed to delete connector {}: {}", connectorName, e.getMessage());
            }
        }
    }

    protected void waitForDataInFirebolt(String tableName, int expectedRowCount) throws SQLException {
        waitForDataInFirebolt(tableName, expectedRowCount, DEFAULT_TIMEOUT);
    }

    protected void waitForDataInFirebolt(String tableName, int expectedRowCount, Duration maxWaitDuration) throws SQLException {
        log.info("Waiting for {} rows in Firebolt table '{}'... Will wait for :{}", expectedRowCount, tableName, maxWaitDuration);

        await()
            .atMost(maxWaitDuration)
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> {
                try {
                    int count = fireboltDefaultDbClient.countRows(tableName);
                    log.debug("Current row count in table '{}': {}", tableName, count);
                    return count >= expectedRowCount;
                } catch (SQLException e) {
                    log.debug("Error querying Firebolt table: {}", e.getMessage());
                    return false;
                }
            });

        log.info("Found expected data in Firebolt table '{}'", tableName);
    }

    /**
     * Generates a unique connector name for test runs.
     * @param connectorType The type/name of the connector (e.g., "integer-serializer-test")
     * @return A unique connector name with a random suffix
     */
    protected void generateUniqueConnectorName(String connectorType) {
        String testId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        this.testConnectorName = connectorType + "-connector-" + testId;
    }

    protected static String getJdbcConnectionUrl() {
        String systemPropertyUrl = System.getProperty("jdbc.connection.url");
        if (systemPropertyUrl != null && !systemPropertyUrl.trim().isEmpty()) {
            log.info("Using JDBC connection URL from system property: {}", systemPropertyUrl);
            return systemPropertyUrl;
        }
        
        // Default to local Firebolt Core for integration tests
        String defaultUrl = "jdbc:firebolt:" + DEFAULT_DATABASE_NAME + "?url=http://firebolt-core.local:3473";
        log.info("Using default JDBC connection URL: {}", defaultUrl);
        return defaultUrl;
    }

    protected String getClientId() {
        String clientId = System.getProperty("clientId");
        if (clientId != null && !clientId.trim().isEmpty()) {
            log.info("Using client ID from system property");
            return clientId;
        }
        
        log.debug("No client ID system property set, returning null");
        return null;
    }

    protected String getClientSecret() {
        String clientSecret = System.getProperty("clientSecret");
        if (clientSecret != null && !clientSecret.trim().isEmpty()) {
            log.info("Using client secret from system property");
            return clientSecret;
        }
        
        log.debug("No client secret system property set, returning null");
        return null;
    }

    protected static String getDatabaseName() {
        // Use a URL with the same structure as the default but with a non-existing database
        return JdbcConnectionParser.getDatabase(getJdbcConnectionUrl());

    }

    protected void assertEqualsBigDecimal(BigDecimal expected, BigDecimal actual, int recordIndex) {
        // Null handling verification for optional numeric
        if (expected == null) {
            assertNull(actual,
                    "OptionalNumeric should be null at index " + recordIndex);
        } else {
            assertEquals(0, expected.compareTo(actual),
                    "OptionalNumeric mismatch at index " + recordIndex +
                            " (expected: " + expected + ", actual: " + actual + ")");
        }

    }

    protected Supplier<String> simpleRecordTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"value\" TEXT NOT NULL " +
                ")";
    }

    protected Supplier<String> jsonSimpleRecordSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Column Name Casing Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"id\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"value\": {\n" +
                "      \"type\": \"string\"\n" +
                "    }" +
                "  },\n" +
                "  \"required\": [\"ID\", \"Text\", \"localdate\", \"bigInt\"]\n" +
                "}";
    }

    protected void verifyRecords(String tableName, List<SimpleRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(tableName);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"value\" " +
                        "FROM \"%s\" ORDER BY \"id\"", tableName);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                SimpleRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getValue(), rs.getString("value"));

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    protected void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    /**
     * Most of the datatypes test would run the schema and schemaless integration with both sql and binary ingestion type
     * This represents a provider for those tests
     * @return
     */
    protected static Stream<Arguments> ingestionTypesWithOrWithoutNulls() {
        return Stream.of(
                Arguments.of(INCLUDE_NULL_SERIALIZED_VALUES, Map.of("ingestion.type", "sql"), "sql ingestion with null values"),
                Arguments.of(DO_NOT_INCLUDE_NULL_SERIALIZED_VALUES, Map.of("ingestion.type", "sql"), "sql ingestion without null values"),
                Arguments.of(INCLUDE_NULL_SERIALIZED_VALUES, Map.of("ingestion.type", "binary"), "binary ingestion with null values"),
                Arguments.of(DO_NOT_INCLUDE_NULL_SERIALIZED_VALUES, Map.of("ingestion.type", "binary"), "binary ingestion without null values"));
    }

    protected static Stream<Arguments> ingestionTypes() {
        return Stream.of(
                Arguments.of(Map.of("ingestion.type", "sql"), "sql ingestion with null values"),
                Arguments.of(Map.of("ingestion.type", "binary"), "binary ingestion with null values"));
    }

}