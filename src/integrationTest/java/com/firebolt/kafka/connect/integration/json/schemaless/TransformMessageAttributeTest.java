package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TransformMessageAttributeTest extends SchemalessBaseIntegrationTest {

    // All data types test constants
    private static final String FIELD_TRANSFORMATION_TABLE_NAME = "field_transformation_test_table_schemaless";
    private static final String FIELD_TRANSFORMATION_TOPIC_NAME = "field-transformation-test-topic-schemaless";

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("field-transformation-test-connector-schemaless");

        // add the transformers here
        Map<String, String> connectorPropertiesOverride = new HashMap<>();
        connectorPropertiesOverride.put("transforms", "renameUserId,renameOrderStatus");
        connectorPropertiesOverride.put("transforms.renameUserId.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        connectorPropertiesOverride.put("transforms.renameUserId.renames", "userId:user_id");
        connectorPropertiesOverride.put("transforms.renameUserId.predicate", "isOurTopic");
        connectorPropertiesOverride.put("transforms.renameOrderStatus.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        connectorPropertiesOverride.put("transforms.renameOrderStatus.renames", "orderStatus:order_status");
        connectorPropertiesOverride.put("transforms.renameOrderStatus.predicate", "isOurTopic");

        // Limit transformations to this topic only
        connectorPropertiesOverride.put("predicates", "isOurTopic");
        connectorPropertiesOverride.put("predicates.isOurTopic.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        connectorPropertiesOverride.put("predicates.isOurTopic.pattern", FIELD_TRANSFORMATION_TOPIC_NAME);

        // Setup test resources using centralized method
        setupSchemalessTestResources(FIELD_TRANSFORMATION_TOPIC_NAME, FIELD_TRANSFORMATION_TABLE_NAME, fieldTransformationTableSchema(), connectorPropertiesOverride);
    }

    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }

        // Clean up test resources
        cleanupSchemalessTestResources(FIELD_TRANSFORMATION_TABLE_NAME, FIELD_TRANSFORMATION_TOPIC_NAME);

        super.tearDown();
    }

    @Test
    void testAllDataTypesJsonSchemaSerializationAndKafkaConnectProcessing() throws Exception {
        producer =  initializeSchemalessJsonProducer();

        // Generate 5 test messages with different data patterns
        List<FieldTransformationRecord> testRecords = generateFieldTransformationTestRecords();

        // Publish messages to Kafka using JSON serialization
        publishAllMessages(testRecords);

        // Wait for connector to process messages
        waitForDataInFirebolt(FIELD_TRANSFORMATION_TABLE_NAME, testRecords.size());

        // Verify data was written to Firebolt table
        verifyAllRecordsInFirebolt(testRecords);
    }

    private Supplier<String> fieldTransformationTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                // Numeric types
                "\"id\" INTEGER NOT NULL, " +
                "\"user_id\" BIGINT, " +
                "\"order_status\" TEXT, " +
                "\"locationAddress\" TEXT )";
    }

    /**
     * Generates test records for all data types testing.
     */
    private List<FieldTransformationRecord> generateFieldTransformationTestRecords() {
        return Arrays.asList(
                aValidFieldTransformationTestRecord(1)
                        .build(),
                aValidFieldTransformationTestRecord(2)
                        .userId(2000L)
                        .orderStatus("in_progress")
                        .locationAddress("Cluj Napoca")
                        .build(),
                aValidFieldTransformationTestRecord(3)
                        .userId(3000L)
                        .build(),
                aValidFieldTransformationTestRecord(4)
                        .userId(4000L)
                        .locationAddress("Las Vegas")
                        .build()
        );
    }

    /**
     * Helper method to create a valid AllDataTypesTestRecord with default values.
     */
    private FieldTransformationRecord.FieldTransformationRecordBuilder aValidFieldTransformationTestRecord(int id) {
        return FieldTransformationRecord.builder()
                // Numeric types
                .id(id)
                .userId(1000L)
                .orderStatus("paid")
                .locationAddress("San Francisco");
    }

    /**
     * Publishes all data types messages to Kafka topic using JSON Schema serialization.
     */
    private void publishAllMessages(List<FieldTransformationRecord> records) throws Exception {
        for (FieldTransformationRecord record : records) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(FIELD_TRANSFORMATION_TOPIC_NAME, String.valueOf(record.getId()), mapper.writeValueAsString(record));

            producer.send(producerRecord).get(); // Wait for each message to be sent
        }
    }

    /**
     * Verifies that all data types records were properly written to Firebolt.
     */
    private void verifyAllRecordsInFirebolt(List<FieldTransformationRecord> expectedRecords) throws SQLException {

        // Verify specific records by checking the integer column (which is unique)
        String selectQuery = "SELECT \"id\", \"user_id\", \"order_status\", \"locationAddress\" FROM \"" + FIELD_TRANSFORMATION_TABLE_NAME + "\" ORDER BY \"id\"";

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                FieldTransformationRecord expected = expectedRecords.get(recordIndex);
                assertEquals(rs.getInt("id"), expected.getId());
                assertEquals(rs.getLong("user_id"), expected.getUserId());
                assertEquals(rs.getString("order_status"), expected.getOrderStatus());
                assertEquals(rs.getString("locationAddress"), expected.getLocationAddress());

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected " + expectedRecords.size() + " records but processed " + recordIndex);
        }
    }

    @Builder
    @Data
    private static class FieldTransformationRecord {
        private Integer id;
        private Long userId;
        private String orderStatus;
        private String locationAddress;
    }

}
