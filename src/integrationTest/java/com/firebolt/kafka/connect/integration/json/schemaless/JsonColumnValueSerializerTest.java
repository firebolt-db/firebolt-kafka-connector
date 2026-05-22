package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.utils.TestTag;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class JsonColumnValueSerializerTest extends SchemalessBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("json_as_text_column");
    private String TOPIC_NAME = generateTopicName("json-as-text-column");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("json-as-text-column");

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, jsonAsColumnTableSchema());
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
    void jsonAsColumnValues() throws Exception {
        producer = initializeSchemalessJsonProducer();

        List<SimpleRecord> testRecords = asList(
                // Complete record with typical values
                aValidTestRecord(1)
                        .jsonValue(objectMapper.writeValueAsString(InnerJson.builder().key("key1").entry(1).build()))
                        .build(),
                aValidTestRecord(2)
                        .jsonValue(objectMapper.writeValueAsString(InnerJson.builder().key("key2").entry(2).build()))
                        .build()

        );

        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        verifyTextRecordsInFirebolt(testRecords);

        // First verification: extract the 'key' value from inner JSON and validate it
        InnerJson expectedInner = objectMapper.readValue(testRecords.get(0).getJsonValue(), InnerJson.class);

        String selectKeyValueQuery = String.format(
                "SELECT JSON_POINTER_EXTRACT_TEXT(t.\"jsonValue\", '/key') as key FROM \"%s\" as t ORDER BY \"id\"",
                TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectKeyValueQuery)) {
            assertTrue(rs.next(), "Expected at least one row when extracting JSON 'key'");
            assertEquals("key1", rs.getString("key"));
            rs.next();
            assertEquals("key2", rs.getString("key"));
        }

        // Second verification: extract the 'entry' value from inner JSON and validate it
        String selectKeyEntryValueQuery = String.format(
                "SELECT JSON_VALUE(JSON_POINTER_EXTRACT(t.\"jsonValue\", '/entry'))::INT as entry FROM \"%s\" as t ORDER BY \"id\"",
                TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectKeyEntryValueQuery)) {
            assertTrue(rs.next(), "Expected at least one row when extracting JSON 'keyId'");
            assertEquals(1, rs.getInt("entry"));
            rs.next();
            assertEquals(2, rs.getInt("entry"));
        }
    }

    private Supplier<String> jsonAsColumnTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"value\" TEXT NOT NULL, " +
                "\"jsonValue\" TEXT NULL )";
    }

    /**
     * Publishes TextTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "json-column-value-" + record.getId();
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

    /**
     * Verifies that the published text records exist in the Firebolt table with correct null handling.
     */
    private void verifyTextRecordsInFirebolt(List<SimpleRecord> expectedRecords) throws SQLException, JsonProcessingException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"value\", \"jsonValue\" FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                SimpleRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getValue(), rs.getString("value"));

                assertEquals(expected.getJsonValue(), rs.getString("jsonValue"), "Expected: " + objectMapper.writeValueAsString(expected.getJsonValue()) + " but was " + rs.getString("jsonValue"));
                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private SimpleRecord.SimpleRecordBuilder aValidTestRecord(int recordId) {
        return SimpleRecord.builder()
                .id(recordId)
                .value("value" + recordId);
    }


    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class SimpleRecord {

        private Integer id;

        private String value;

        private String jsonValue;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class InnerJson {

        private String key;

        private int entry;
    }

}
