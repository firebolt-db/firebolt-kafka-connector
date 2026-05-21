package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class SchemalessWithTransformsTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("message_transforms_table");
    private static final String TOPIC_NAME = generateTopicName("message-transforms-topic");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("message-transforms-test");

        // set the transforms where we cast the integers to int32
        Map<String, String> connectorOverrides = new HashMap<>();

        connectorOverrides.put("transforms", "castIntegers");
        connectorOverrides.put("transforms.castIntegers.type","org.apache.kafka.connect.transforms.Cast$Value");
        connectorOverrides.put("transforms.castIntegers.spec", "colInteger:int32,colShort:int16,colByte:int8");
        connectorOverrides.put("transforms.castIntegers.predicate", "isOurTopic");

        connectorOverrides.put("predicates", "isOurTopic");
        connectorOverrides.put("predicates.isOurTopic.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
        connectorOverrides.put("predicates.isOurTopic.pattern", TOPIC_NAME);

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, integerTableSchema(), connectorOverrides);
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
    void canProcessIntegersWithTransforms() throws Exception {
        producer = initializeSchemalessJsonProducer();

        List<TestRecord> testRecords = List.of(
                aValidTestRecord(1)
                        .colInteger(1000)
                        .colShort((short) 100)
                        .colByte((byte) 10)
                        .build(),
                aValidTestRecord(2)
                        .colInteger(2000)
                        .colShort((short) 200)
                        .colByte((byte) 20)
                        .build()
        );

        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        verifyIntegerRecordsInFirebolt(testRecords);
    }

    private Supplier<String> integerTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"colInteger\" INTEGER NOT NULL, " +
                "\"colShort\" INTEGER NOT NULL, " +
                "\"colByte\" INTEGER NOT NULL " +
                ")";
    }

    private void publishMessages(List<TestRecord> records) throws Exception {
        for (TestRecord record : records) {
            String key = "integer-test-key-" + record.getRecordId();
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(TOPIC_NAME, key, mapper.writeValueAsString(record));

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message with key {}: {}", key, exception.getMessage());
                } else {
                    log.debug("Successfully sent message with key {} to partition {} at offset {}",
                            key, metadata.partition(), metadata.offset());
                }
            }).get();
        }

        producer.flush();
    }

    private void verifyIntegerRecordsInFirebolt(List<TestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"recordId\", \"colInteger\", \"colShort\", \"colByte\"  " +
                        "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                TestRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getRecordId(), rs.getInt("recordId"));
                assertEquals(expected.getColInteger(), rs.getInt("colInteger"));
                assertEquals(expected.getColShort(), (short) rs.getInt("colShort"));
                assertEquals(expected.getColByte(), (byte) rs.getInt("colByte"));

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private TestRecord.TestRecordBuilder aValidTestRecord(int recordId) {
        return TestRecord.builder()
                .recordId(recordId);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class TestRecord {
        private Integer recordId;
        private Integer colInteger;
        private Short colShort;
        private Byte colByte;
    }

}
