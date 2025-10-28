package com.firebolt.kafka.connect.integration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
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
public class IngestionConfigurationIntegrationTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("ingestion_config_table");
    private static final String TOPIC_NAME = generateTopicName("ingestion-config-topic");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("ingestion-config-test");
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }

        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);

        super.tearDown();
    }

    @Test
    void testIngestionConfig() throws Exception {
        Map<String, String> connectorOverrideProperties = new HashMap<>();

        // wait for 5 seconds or when the number of min bytes (in this case 16MB). It will never be accumulated since we
        // have 3 small messages so it should wait for 5 seconds
        connectorOverrideProperties.put("consumer.override.fetch.max.bytes", "18874368");
        connectorOverrideProperties.put("consumer.override.fetch.min.bytes", "16777216");
        connectorOverrideProperties.put("consumer.override.fetch.max.wait.ms", "5000");
        connectorOverrideProperties.put("consumer.poll.timeout.ms", "5000");

        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, ingestionConfigTableSchema(), connectorOverrideProperties);

        producer = initializeSchemalessJsonProducer();

        List<SimpleRecord> testRecords = Arrays.asList(
                aValidTestRecord(1, "my comment1"),
                aValidTestRecord(2, "my comment2"),
                aValidTestRecord(3, "my comment3")
        );

        long testStartTime = Instant.now().toEpochMilli();

        // publish the messages to kafka topic
        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // check that all the values are in the target table. Ingestion should happen at least 5 seconds after
        verifyTargetTableResults(testRecords, testStartTime + TimeUnit.SECONDS.toMillis(5));
    }

    private Supplier<String> ingestionConfigTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"comment\" TEXT NOT NULL," +
                "\"ingested_at\" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP )";
    }

    private void publishMessages(List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "post-processing-script-test-key-" + record.getId();
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(TOPIC_NAME, key, objectMapper.writeValueAsString(record));

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

    private void verifyTargetTableResults(List<SimpleRecord> expectedRecords, long afterDateInMillis) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"comment\", \"ingested_at\" " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                SimpleRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getComment(), rs.getString("comment"));
                long actualIngestedTime = rs.getTimestamp("ingested_at").toInstant().toEpochMilli();

                // all the records should have been ingested after this date
                assertTrue(actualIngestedTime > afterDateInMillis,"Ingestion time was " + actualIngestedTime + ", expected to be after: " + afterDateInMillis);
                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private SimpleRecord aValidTestRecord(int recordId, String comment) {
        return SimpleRecord.builder()
                .id(recordId)
                .comment(comment)
                .build();
    }

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class SimpleRecord {

        private Integer id;

        private String comment;

    }

}
