package com.firebolt.kafka.connect.integration.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
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
public class ColumnNameCasingTest extends BaseIntegrationTest {

    private static final String TABLE_NAME = "column_name_casing_table";
    private static final String TOPIC_NAME = "column-name-casing-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, ColumnNameCaseRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("column-name-casing-test");

        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                columnNameCasingTableSchema(), jsonColumnNameCaseSchema());
    }

    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }

        // Clean up test resources
        cleanupTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);

        super.tearDown();
    }

    @Test
    void testCaseInsensitiveColumnNamesSerialization() throws Exception {
        producer = initializeJsonProducer();

        List<ColumnNameCaseRecord> testRecords = createTestRecords();

        // publish the messages to kafka topic
        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // check that all the records have the expected value
        verifyColumnCaseRecords(testRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<ColumnNameCaseRecord> createTestRecords() {
        return Arrays.asList(
                aValidTestRecord(1).build()
        );
    }

    private Supplier<String> columnNameCasingTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"TEXT\" TEXT NOT NULL, " +
                "\"localDate\" DATE NOT NULL, " +
                "\"BigInt\" BIGINT NOT NULL " +
                ")";
    }

    private Supplier<String> jsonColumnNameCaseSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Column Name Casing Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"ID\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"Text\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"localdate\": {\n" +
                "      \"type\": \"string\",\n" +
                "      \"format\": \"date\"" +
                "    },\n" +
                "    \"bigInt\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"format\": \"int64\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"ID\", \"Text\", \"localdate\", \"bigInt\"]\n" +
                "}";
    }

    private void publishMessages(List<ColumnNameCaseRecord> records) throws Exception {
        for (ColumnNameCaseRecord record : records) {
            String key = "column-name-casing-test-key-" + record.getId();
            ProducerRecord<String, ColumnNameCaseRecord> producerRecord =
                    new ProducerRecord<>(TOPIC_NAME, key, record);

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

    private void verifyColumnCaseRecords(List<ColumnNameCaseRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"TEXT\", \"localDate\", \"BigInt\" " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                ColumnNameCaseRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getText(), rs.getString("TEXT"));
                assertEquals(expected.getLocalDate(), rs.getDate("localDate").toLocalDate());
                assertEquals(expected.getBigInt(), rs.getObject("BigInt", Long.class));

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private ColumnNameCaseRecord.ColumnNameCaseRecordBuilder aValidTestRecord(int recordId) {
        return ColumnNameCaseRecord.builder()
                .id(recordId)
                .bigInt(100L)
                .text("some text")
                .localDate(LocalDate.of(2024, 12, 31));
    }

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ColumnNameCaseRecord {

        @JsonProperty("ID")
        private Integer id;

        @JsonProperty("Text")
        private String text;

        @JsonProperty("localdate")
        private LocalDate localDate;

        @JsonProperty("bigInt")
        private Long bigInt;
    }
}
