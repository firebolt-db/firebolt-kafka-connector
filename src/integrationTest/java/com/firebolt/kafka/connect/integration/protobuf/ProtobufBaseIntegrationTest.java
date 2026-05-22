package com.firebolt.kafka.connect.integration.protobuf;

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
public class ProtobufBaseIntegrationTest extends BaseIntegrationTest {

    protected static final String SCHEMA_REGISTRY_URL =
            System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

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

    protected void setupProtobufTestResources(
            String topicName,
            String tableName,
            String schemaSubject,
            java.util.function.Supplier<String> tableSchemaSupplier,
            java.util.function.Supplier<String> protobufSchemaSupplier) {
        setupProtobufTestResources(
                topicName, tableName, schemaSubject,
                tableSchemaSupplier, protobufSchemaSupplier, Collections.emptyMap());
    }

    protected void setupProtobufTestResources(
            String topicName,
            String tableName,
            String schemaSubject,
            java.util.function.Supplier<String> tableSchemaSupplier,
            java.util.function.Supplier<String> protobufSchemaSupplier,
            Map<String, String> connectorDefinitionOverride) {
        try {
            cleanupProtobufTestResources(tableName, topicName, schemaSubject);

            createTable(tableSchemaSupplier, tableName);

            log.info("Creating Kafka topic: {}", topicName);
            createKafkaTopic(topicName);

            log.info("Registering PROTOBUF schema for subject: {}", schemaSubject);
            String protobufSchema = protobufSchemaSupplier.get();
            int schemaId = getSchemaRegistryClient().registerSchema(schemaSubject, protobufSchema, "PROTOBUF");
            log.info("Successfully registered PROTOBUF schema with ID: {}", schemaId);

            log.info("Registering Kafka Connect connector: {}", testConnectorName);
            registerProtobufConnector(
                    testConnectorName, topicName, topicName + ":" + tableName, connectorDefinitionOverride);
        } catch (Exception e) {
            log.error("Failed to set up Protobuf test resources: {}", e.getMessage());
            throw new RuntimeException("Protobuf test resources setup failed", e);
        }
    }

    protected void cleanupProtobufTestResources(String tableName, String topicName, String schemaSubject) {
        safelyDeleteConnector(testConnectorName);
        safelyDropTable(tableName);
        safelyDeleteKafkaTopic(topicName);
        safelyDeleteProtobufSchema(schemaSubject);
    }

    protected void registerProtobufConnector(
            String connectorName,
            String topics,
            String topicToTableMappings,
            Map<String, String> connectorDefinitionOverride) throws Exception {
        Map<String, Object> connectorConfig = createBasicConnectorDefinition(topics, topicToTableMappings);

        // Value converter: Protobuf — ProtobufConverter produces Struct objects handled by SchemaBasedRecordConverter
        connectorConfig.put("value.converter", "io.confluent.connect.protobuf.ProtobufConverter");
        connectorConfig.put("value.converter.schema.registry.url", "http://schema-registry:8081");
        connectorConfig.put("value.converter.auto.register.schemas", "false");
        connectorConfig.put("value.converter.use.latest.version", "true");
        connectorConfig.put("value.converter.latest.compatibility.strict", "false");
        connectorConfig.put("schemas.enable", "true");

        // Flatten Protobuf `oneof` into a separate top-level Connect field per member, mirroring
        // ClickHouse's behaviour (see https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink#protobuf-schema-support).
        // Each member must have a corresponding column in the Firebolt table; only the member set
        // on the wire receives a non-null value and the others land as SQL NULL. Disabling the
        // discriminator index keeps the resulting Connect schema minimal so the connector does
        // not need to map an extra synthetic column.
        connectorConfig.put("value.converter.flatten.unions", "true");
        connectorConfig.put("value.converter.generate.index.for.unions", "false");

        if (connectorDefinitionOverride != null && !connectorDefinitionOverride.isEmpty()) {
            log.info("Applying connector definition override");
            connectorConfig.putAll(connectorDefinitionOverride);
        }

        createConnectorAndWaitForItToStart(connectorName, topicToTableMappings, connectorConfig);
    }

    /**
     * Creates a Kafka producer that serializes values with KafkaProtobufSerializer.
     * The serializer queries Schema Registry at registration time but uses the pre-registered schema.
     */
    protected <T extends com.google.protobuf.Message> Producer<String, T> initializeProtobufProducer() {
        Properties props = createBasicProducerProperties(false);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", "false");
        props.put("use.latest.version", "true");
        props.put("latest.compatibility.strict", "false");
        return new KafkaProducer<>(props);
    }

    protected void safelyDeleteProtobufSchema(String schemaSubject) {
        try {
            getSchemaRegistryClient().deleteSchema(schemaSubject);
            log.debug("Deleted Protobuf schema: {}", schemaSubject);
        } catch (Exception e) {
            log.warn("Failed to delete Protobuf schema {}: {}", schemaSubject, e.getMessage());
        }
    }

    protected SchemaRegistryClient getSchemaRegistryClient() {
        return schemaRegistryClient;
    }
}

