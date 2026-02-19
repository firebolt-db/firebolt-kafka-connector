package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.clients.SchemaRegistryClient;
import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
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
public class AvroBaseIntegrationTest extends BaseIntegrationTest {

    protected static final String SCHEMA_REGISTRY_URL = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    protected SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        this.schemaRegistryClient = new SchemaRegistryClient(SCHEMA_REGISTRY_URL, httpClient, objectMapper);
    }

    @AfterEach
    protected void tearDown() {
        if (schemaRegistryClient != null) {
            log.debug("Closing Schema Registry client for test: {}", testName);
            schemaRegistryClient.close();
            schemaRegistryClient = null;
        }
        super.tearDown();
    }

    protected void setupAvroTestResources(
            String topicName,
            String tableName,
            String schemaSubject,
            java.util.function.Supplier<String> tableSchemaSupplier,
            java.util.function.Supplier<String> avroSchemaSupplier) {
        setupAvroTestResources(topicName, tableName, schemaSubject, tableSchemaSupplier, avroSchemaSupplier, Collections.emptyMap());
    }

    protected void setupAvroTestResources(
            String topicName,
            String tableName,
            String schemaSubject,
            java.util.function.Supplier<String> tableSchemaSupplier,
            java.util.function.Supplier<String> avroSchemaSupplier,
            Map<String, String> connectorDefinitionOverride) {
        try {
            // Clean up previous resources
            cleanupAvroTestResources(tableName, topicName, schemaSubject);

            // Create Firebolt table
            createTable(tableSchemaSupplier, tableName);

            // Create topic
            log.info("Creating Kafka topic: {}", topicName);
            createKafkaTopic(topicName);

            // Register Avro schema
            log.info("Registering AVRO schema for subject: {}", schemaSubject);
            String avroSchema = avroSchemaSupplier.get();
            int schemaId = getSchemaRegistryClient().registerSchema(schemaSubject, avroSchema, "AVRO");
            log.info("Successfully registered AVRO schema with ID: {}", schemaId);

            // Register connector
            log.info("Registering Kafka Connect connector: {}", testConnectorName);
            registerAvroConnector(testConnectorName, topicName, topicName + ":" + tableName, connectorDefinitionOverride);
        } catch (Exception e) {
            log.error("Failed to set up AVRO test resources: {}", e.getMessage());
            throw new RuntimeException("AVRO test resources setup failed", e);
        }
    }

    protected void cleanupAvroTestResources(String tableName, String topicName, String schemaSubject) {
        // Clean up connector
        safelyDeleteConnector(testConnectorName);
        // Clean up Firebolt table
        safelyDropTable(tableName);
        // Clean up Kafka topic
        safelyDeleteKafkaTopic(topicName);
        // Clean up schema registry
        safelyDeleteAvroSchema(schemaSubject);
    }

    protected void registerAvroConnector(String connectorName, String topics, String topicToTableMappings, Map<String, String> connectorDefinitionOverride) throws Exception {
        Map<String, Object> connectorConfig = createBasicConnectorDefinition(topics, topicToTableMappings);

        // Value converter: Avro
        connectorConfig.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        connectorConfig.put("value.converter.schema.registry.url", "http://schema-registry:8081");
        connectorConfig.put("value.converter.auto.register.schemas", "false");
        connectorConfig.put("value.converter.use.latest.version", "true");
        connectorConfig.put("value.converter.latest.compatibility.strict", "false");
        connectorConfig.put("schemas.enable", "true");

        if (connectorDefinitionOverride != null && !connectorDefinitionOverride.isEmpty()) {
            log.info("Applying the connector definition override");
            connectorConfig.putAll(connectorDefinitionOverride);
        }

        createConnectorAndWaitForItToStart(connectorName, topicToTableMappings, connectorConfig);
    }

    protected <T> Producer<String, T> initializeAvroProducer() {
        Properties props = createBasicProducerProperties(true);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        // Schema Registry configuration
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", "false");
        props.put("use.latest.version", "true");
        props.put("latest.compatibility.strict", "false");
        return new KafkaProducer<>(props);
    }

    protected void safelyDeleteAvroSchema(String schemaName) {
        try {
            getSchemaRegistryClient().deleteSchema(schemaName);
            log.debug("Deleted schema: {}", schemaName);
        } catch (Exception e) {
            log.warn("Failed to delete schema {}: {}", schemaName, e.getMessage());
        }
    }

    protected SchemaRegistryClient getSchemaRegistryClient() {
        return schemaRegistryClient;
    }
}
