package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.OffsetDateTimeSerializer;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SchemalessBaseIntegrationTest extends BaseIntegrationTest {

    private static SimpleModule jtm = new JavaTimeModule();

    protected static ObjectMapper mapper = new ObjectMapper()
            .registerModule(jtm)
            .enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);// no scientific notation

    protected void generateUniqueConnectorName(String connectorType) {
        super.generateUniqueConnectorName(connectorType + "-schemaless");
    }

    protected static String generateTableName(String name) {
        return name + "_schemaless";
    }

    protected static String generateTopicName(String name) {
        return name + "-schemaless";
    }

    /**
     * Centralized setup method for test resources.
     * This method handles setup of Firebolt table, Kafka topic, and Kafka Connect connector.
     *
     * @param topicName The name of the Kafka topic to create
     * @param tableName The name of the Firebolt table to create
     * @param tableSchemaSupplier Supplier that provides the Firebolt table schema definition
     */
    protected void setupSchemalessTestResources(
            String topicName,
            String tableName,
            java.util.function.Supplier<String> tableSchemaSupplier) {
        setupSchemalessTestResources(topicName, tableName, tableSchemaSupplier, Collections.emptyMap());
    }

    /**
     * Centralized setup method for test resources.
     * This method handles setup of Firebolt table, Kafka topic, and Kafka Connect connector.
     *
     * @param topicName The name of the Kafka topic to create
     * @param tableName The name of the Firebolt table to create
     * @param tableSchemaSupplier Supplier that provides the Firebolt table schema definition
     */
    protected void setupSchemalessTestResources(
            String topicName,
            String tableName,
            java.util.function.Supplier<String> tableSchemaSupplier,
            Map<String, String> connectorDefinitionOverride) {

        try {
            // Clean up any existing resources from previous test runs
            cleanupSchemalessTestResources(tableName, topicName);

            // Create the test table with the provided schema
            log.info("Creating Firebolt test table: {}", tableName);
            String createTableSql = String.format(tableSchemaSupplier.get(), tableName);
            fireboltDefaultDbClient.executeUpdate(createTableSql);
            log.info("Created Firebolt test table with schema");

            // Create Kafka topic
            log.info("Creating Kafka topic: {}", topicName);
            createKafkaTopic(topicName);

            // Register the Kafka Connect connector
            log.info("Registering Kafka Connect connector: {}", testConnectorName);
            registerSchemalessJsonConnector(testConnectorName, topicName, topicName + ":" + tableName, connectorDefinitionOverride);

        } catch (Exception e) {
            log.error("Failed to set up test resources: {}", e.getMessage());
            throw new RuntimeException("Test resources setup failed", e);
        }
    }

    /**
     * Centralized cleanup method for test resources.
     * This method handles cleanup of connector, Firebolt table and Kafka topic
     *
     * @param tableName The name of the Firebolt table to drop
     * @param topicName The name of the Kafka topic to delete
     */
    protected void cleanupSchemalessTestResources(String tableName, String topicName) {
        // Clean up connector
        safelyDeleteConnector(testConnectorName);

        // Clean up Firebolt table
        safelyDropTable(tableName);

        // Clean up Kafka topic
        safelyDeleteKafkaTopic(topicName);
    }

    /**
     * Registers a Kafka Connect connector for JSON Schemaless processing with Firebolt.
     * This method creates a standard JSON Schema connector configuration suitable for most tests.
     *
     * @param connectorName the name of the connector to create
     * @param topics the Kafka topics to consume from (comma-separated if multiple)
     * @param topicToTableMappings the topic-to-table mappings (e.g., "topic1:table1,topic2:table2")
     * @throws Exception if connector registration fails
     */
    protected void registerSchemalessJsonConnector(String connectorName, String topics, String topicToTableMappings, Map<String, String> connectorDefinitionOverride) throws Exception {
        Map<String, Object> connectorConfig = new HashMap<>();

        // Kafka Connect core properties
        connectorConfig.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");
        connectorConfig.put("tasks.max", "1");
        connectorConfig.put("topics", topics);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");

        connectorConfig.put("schemas.enable", "false");

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

        // before creating the configuration apply definition override
        if (connectorDefinitionOverride != null && !connectorDefinitionOverride.isEmpty()) {
            log.info("Applying the connector definition override");
            connectorConfig.putAll(connectorDefinitionOverride);
        }

        createConnectorAndWaitForItToStart(connectorName, topicToTableMappings, connectorConfig);
    }

    /**
     * Initializes a Kafka producer for JSON Schema serialization with default null handling (nulls included).
     * This method creates a producer configured to produce plain json
     *
     * @param <T> the type of the record values to be produced
     * @return a new KafkaProducer configured for JSON Schema serialization
     */
    protected <T> Producer<String, T> initializeSchemalessJsonProducer() {
        return initializeSchemalessJsonProducer(true); // Default: include nulls in JSON
    }

    /**
     * Initializes a Kafka producer for JSON Schema serialization with configurable null handling.
     *
     * @param <T> the type of the record values to be produced
     * @param includeNulls true to include null values in JSON serialization, false to omit them
     * @return a new KafkaProducer configured for JSON Schema serialization
     */
    protected <T> Producer<String, T> initializeSchemalessJsonProducer(boolean includeNulls) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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

        Producer<String, T> producer = new KafkaProducer<>(props);
        log.info("Kafka JSON Schemaless producer initialized successfully with null handling: includeNulls={}", includeNulls);
        return producer;
    }
}
