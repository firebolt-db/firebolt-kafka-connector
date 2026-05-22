package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.clients.SchemaRegistryClient;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SchemaBaseIntegrationTest extends BaseIntegrationTest {

    protected static final String SCHEMA_REGISTRY_URL = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    protected SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
       super.setUp(testInfo);
       this.schemaRegistryClient = new SchemaRegistryClient(SCHEMA_REGISTRY_URL, httpClient, objectMapper);
    }

    @AfterEach
    protected void tearDown() {
        // Close Schema Registry client if it was created
        if (schemaRegistryClient != null) {
            log.debug("Closing Schema Registry client for test: {}", testName);
            schemaRegistryClient.close();
            schemaRegistryClient = null;
        }

        super.tearDown();
    }

    /**
     * Centralized setup method for test resources.
     * This method handles setup of Firebolt table, Kafka topic, schema registry, and Kafka Connect connector.
     *
     * @param topicName The name of the Kafka topic to create
     * @param tableName The name of the Firebolt table to create
     * @param schemaSubject The name of the schema registry subject to register
     * @param tableSchemaSupplier Supplier that provides the Firebolt table schema definition
     * @param jsonSchemaSupplier Supplier that provides the JSON schema definition for schema registry
     */
    protected void setupTestResources(
            String topicName,
            String tableName,
            String schemaSubject,
            java.util.function.Supplier<String> tableSchemaSupplier,
            java.util.function.Supplier<String> jsonSchemaSupplier) {
        setupTestResources(topicName, tableName, schemaSubject, tableSchemaSupplier, jsonSchemaSupplier, Collections.emptyMap());
    }

    /**
     * Centralized setup method for test resources.
     * This method handles setup of Firebolt table, Kafka topic, schema registry, and Kafka Connect connector.
     *
     * @param topicName The name of the Kafka topic to create
     * @param tableName The name of the Firebolt table to create
     * @param schemaSubject The name of the schema registry subject to register
     * @param tableSchemaSupplier Supplier that provides the Firebolt table schema definition
     * @param jsonSchemaSupplier Supplier that provides the JSON schema definition for schema registry
     */
    protected void setupTestResources(
            String topicName,
            String tableName,
            String schemaSubject,
            java.util.function.Supplier<String> tableSchemaSupplier,
            java.util.function.Supplier<String> jsonSchemaSupplier,
            Map<String, String> connectorDefinitionOverride) {

        try {
            // Clean up any existing resources from previous test runs
            cleanupTestResources(tableName, topicName, schemaSubject);

            // Create the test table with the provided schema
            createTable(tableSchemaSupplier, tableName);

            // Create Kafka topic
            log.info("Creating Kafka topic: {}", topicName);
            createKafkaTopic(topicName);

            // Register JSON schema
            log.info("Registering JSON schema for subject: {}", schemaSubject);
            String jsonSchema = jsonSchemaSupplier.get();
            int schemaId = getSchemaRegistryClient().registerSchema(schemaSubject, jsonSchema, "JSON");
            log.info("Successfully registered JSON schema with ID: {}", schemaId);

            // Register the Kafka Connect connector
            log.info("Registering Kafka Connect connector: {}", testConnectorName);
            registerJsonConnector(testConnectorName, topicName, topicName + ":" + tableName, connectorDefinitionOverride);

        } catch (Exception e) {
            log.error("Failed to set up test resources: {}", e.getMessage());
            throw new RuntimeException("Test resources setup failed", e);
        }
    }

    protected void registerJsonConnector(String connectorName, String topics, String topicToTableMappings) throws Exception {
        registerJsonConnector(connectorName, topics, topicToTableMappings, Collections.emptyMap());
    }

    /**
     * Registers a Kafka Connect connector for JSON Schema processing with Firebolt.
     * This method creates a standard JSON Schema connector configuration suitable for most tests.
     *
     * @param connectorName the name of the connector to create
     * @param topics the Kafka topics to consume from (comma-separated if multiple)
     * @param topicToTableMappings the topic-to-table mappings (e.g., "topic1:table1,topic2:table2")
     * @throws Exception if connector registration fails
     */
    protected void registerJsonConnector(String connectorName, String topics, String topicToTableMappings, Map<String, String> connectorDefinitionOverride) throws Exception {
        Map<String, Object> connectorConfig = createBasicConnectorDefinition(topics, topicToTableMappings);

        // Schema Registry configuration and the value is JsonSchemaConverter
        connectorConfig.put("value.converter", "io.confluent.connect.json.JsonSchemaConverter");

        connectorConfig.put("value.converter.schema.registry.url", "http://schema-registry:8081");
        connectorConfig.put("value.converter.auto.register.schemas", "false");

        connectorConfig.put("value.converter.schemas.enable", "true");
        connectorConfig.put("schemas.enable", "true");

        // before creating the configuration apply definition override
        if (connectorDefinitionOverride != null && !connectorDefinitionOverride.isEmpty()) {
            log.info("Applying the connector definition override");
            connectorConfig.putAll(connectorDefinitionOverride);
        }

        // Create the connector
        createConnectorAndWaitForItToStart(connectorName, topicToTableMappings, connectorConfig);
    }

        /**
         * Initializes a Kafka producer for JSON Schema serialization with default null handling (nulls included).
         * This method creates a producer configured for JSON Schema with proper schema registry integration.
         *
         * @param <T> the type of the record values to be produced
         * @return a new KafkaProducer configured for JSON Schema serialization
         */
    protected <T> Producer<String, T> initializeJsonProducer() {
        return initializeJsonProducer(true); // Default: include nulls in JSON
    }

    /**
     * Initializes a Kafka producer for JSON Schema serialization with configurable null handling.
     * This method creates a producer configured for JSON Schema with proper schema registry integration.
     *
     * @param <T> the type of the record values to be produced
     * @param includeNulls true to include null values in JSON serialization, false to omit them
     * @return a new KafkaProducer configured for JSON Schema serialization
     */
    protected <T> Producer<String, T> initializeJsonProducer(boolean includeNulls) {
        Properties props = createBasicProducerProperties(includeNulls);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

        // Schema Registry configuration
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", "false");
        props.put("use.latest.version", "true");
        props.put("latest.compatibility.strict", "false");

        Producer<String, T> producer = new KafkaProducer<>(props);
        log.info("Kafka JSON Schema producer initialized successfully with null handling: includeNulls={}", includeNulls);
        return producer;
    }

        /**
         * Centralized cleanup method for test resources.
         * This method handles cleanup of connector, Firebolt table, Kafka topic, and schema registry.
         *
         * @param tableName The name of the Firebolt table to drop
         * @param topicName The name of the Kafka topic to delete
         * @param schemaSubject The name of the schema registry subject to delete
         */
    protected void cleanupTestResources(String tableName, String topicName, String schemaSubject) {
        // Clean up connector
        safelyDeleteConnector(testConnectorName);

        // Clean up Firebolt table
        safelyDropTable(tableName);

        // Clean up Kafka topic
        safelyDeleteKafkaTopic(topicName);

        // Clean up schema registry
        safelyDeleteJsonSchema(schemaSubject);
    }

    /**
     * Generates a unique connector name for test runs.
     * @param connectorType The type/name of the connector (e.g., "integer-serializer-test")
     * @return A unique connector name with a random suffix
     */
    protected void generateUniqueConnectorName(String connectorType) {
        super.generateUniqueConnectorName(connectorType + "-schema");
    }

    protected static String generateTableName(String name) {
        String uid = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        return name + "_schema_" + uid;
    }

    protected static String generateTopicName(String name) {
        String uid = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        return name + "-schema-" + uid;
    }

    /**
     * Safely deletes a specific schema from the Schema Registry.
     * This method is useful for test cleanup to remove schemas created during testing.
     *
     * @param schemaName the name of the schema subject to delete
     */
    protected void safelyDeleteJsonSchema(String schemaName) {
        try {
            getSchemaRegistryClient().deleteSchema(schemaName);
            log.debug("Deleted schema: {}", schemaName);
        } catch (Exception e) {
            log.warn("Failed to delete schema {}: {}", schemaName, e.getMessage());
        }
    }

    /**
     * Gets the Schema Registry client for interacting with schema registry operations.
     *
     * @return the SchemaRegistryClient instance
     */
    protected SchemaRegistryClient getSchemaRegistryClient() {
        return schemaRegistryClient;
    }
}
