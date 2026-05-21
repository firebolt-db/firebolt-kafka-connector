package com.firebolt.kafka.connect.integration.customer;

import com.firebolt.kafka.connect.utils.TestTag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.BigIntTestRecord;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * We have a POC/customer with this schema. We should make sure we don't break them with any changes that we do
 *
 * {
 *   "order_id": "o1",
 *   "user_id": "u1",
 *   "event_time": "2025-09-29T08:35:00Z",
 *   "amount": 149.5,
 *   "status": "PAID"
 * }
 */
@Slf4j
@Tag(TestTag.CUSTOMER)
public class Customer1IntegrationTest extends SchemalessBaseIntegrationTest {
    private static final String TABLE_NAME = "customer1_test_table_schemaless";
    private static final String TOPIC_NAME = "customer1-test-topic-schemaless";

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("customer1-serializer-test-schemaless");

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, testRecord1TableSchema());
    }

    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }

        // Clean up test resources
        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);

        super.tearDown();
    }

    @Test
    void testRecord1Serialization() throws Exception {
        producer = initializeSchemalessJsonProducer();

        // Create test records
        List<TestRecord1> testRecords = createTestRecords();

        // Publish messages
        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // Verify records in Firebolt
        verifyTestRecordsInFirebolt(testRecords);
    }

    private List<TestRecord1> createTestRecords() {
        return Arrays.asList(
                TestRecord1.builder()
                        .orderId("o1")
                        .userId("u1")
                        .eventTime("2025-09-29T08:35:00Z")
                        .amount(149.5f)
                        .status("PAID")
                        .build()
        );
    }

    private Supplier<String> testRecord1TableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"order_id\" TEXT NOT NULL, " +
                "\"user_id\" TEXT NOT NULL, " +
                "\"event_time\" TIMESTAMPTZ NOT NULL, " +
                "\"amount\" DOUBLE PRECISION NOT NULL, " +
                "\"status\" TEXT NOT NULL" +
                ")";
    }

    private void publishMessages(List<TestRecord1> records) throws Exception {
        log.info("Publishing {} TestRecord1 records to topic: {}", records.size(), TOPIC_NAME);

        for (TestRecord1 record : records) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(TOPIC_NAME, String.valueOf(record.getOrderId()), mapper.writeValueAsString(record));

            producer.send(producerRecord).get();
        }
    }

    private void verifyTestRecordsInFirebolt(List<TestRecord1> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"order_id\", \"user_id\", \"event_time\", " +
                        "\"amount\", \"status\" " +
                        "FROM \"%s\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                TestRecord1 expected = expectedRecords.get(recordIndex);

                // Verify each field
                String actualOrderId = rs.getString("order_id");
                assertEquals(expected.getOrderId(), actualOrderId);

                String actualUserId = rs.getString("user_id");
                assertEquals(expected.getUserId(), actualUserId);

                String actualEventTime = rs.getTimestamp("event_time").toInstant().toString();
                assertEquals(expected.getEventTime(), actualEventTime);

                Float actualAmount = rs.getFloat("amount");
                assertEquals(expected.getAmount(), actualAmount);

                String actualStatus = rs.getString("status");
                assertEquals(expected.getStatus(), actualStatus);
                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    @Builder
    @Getter
    private static class TestRecord1 {
        @JsonProperty("order_id")
        private String orderId;
        @JsonProperty("user_id")
        private String userId;
        @JsonProperty("event_time")
        private String eventTime;
        private Float amount;
        private String status;
    }
}
