package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.utils.TestTag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.CONNECTOR)
public class ColumnNameTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("column_name_table");
    private static final String TOPIC_NAME = generateTopicName("column-name-topic");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("column-name-test");
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

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void testColumnNamesSerialization(Map<String, String> connectorOverrides, String description) throws Exception {
        log.info("Running test with column names (case-insensitive and special chars) {}", description);

        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, columnNameCombinedTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer();

        List<ColumnNameRecord> testRecords = List.of(aValidColumnNameRecord(1).build());

        // publish the messages to kafka topic
        publishColumnNameMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // check that all the records have the expected value
        verifyColumnRecords(testRecords);
    }

    private Supplier<String> columnNameCombinedTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"TEXT\" TEXT NOT NULL, " +
                "\"localDate\" DATE NOT NULL, " +
                "\"BigInt\" BIGINT NOT NULL, " +
                "\"column-with-dashes\" TEXT NULL, " +
                "\"column.with.dots\" TEXT NULL, " +
                "\"column with spaces\" TEXT NULL, " +
                "\"column_with_underscores\" TEXT NULL, " +
                "\"case-insensitive-column-with-dashes\" TEXT NULL, " +
                "\"über\" TEXT NULL " +
                ")";
    }

    private void publishColumnNameMessages(List<ColumnNameRecord> records) throws Exception {
        List<ProducerRecord<String,String>> producerRecords = records.stream()
                .map(record -> {
                    String key = "column-name-test-key-" + record.getId();
                    try {
                        return new ProducerRecord<>(TOPIC_NAME, key, mapper.writeValueAsString(record));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                })
                .collect(Collectors.toList());

        publishRecords(producerRecords);
    }

    private void publishRecords(List<ProducerRecord<String,String>> records) throws Exception {
        for (ProducerRecord<String,String> record : records) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message with key {}: {}", record.key(), exception.getMessage());
                } else {
                    log.debug("Successfully sent message with key {} to partition {} at offset {}",
                            record.key(), metadata.partition(), metadata.offset());
                }
            }).get();
        }

        producer.flush();
    }

    private void verifyColumnRecords(List<ColumnNameRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"TEXT\", \"localDate\", \"BigInt\", " +
                        "\"column-with-dashes\", \"column.with.dots\", \"column with spaces\", \"column_with_underscores\", " +
                        "\"case-insensitive-column-with-dashes\", \"über\" " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                ColumnNameRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getText(), rs.getString("TEXT"));
                assertEquals(expected.getLocalDate(), rs.getDate("localDate").toLocalDate());
                assertEquals(expected.getBigInt(), rs.getObject("BigInt", Long.class));
                assertEquals(expected.getColumnWithDashes(), rs.getString("column-with-dashes"));
                assertEquals(expected.getColumnWithDots(), rs.getString("column.with.dots"));
                assertEquals(expected.getColumnWithSpaces(), rs.getString("column with spaces"));
                assertEquals(expected.getColumnWithUnderscore(), rs.getString("column_with_underscores"));
                assertEquals(expected.getCaseInsensitiveColumnWithDashes(), rs.getString("case-insensitive-column-with-dashes"));
                assertEquals(expected.getUeber(), rs.getString("über"));

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private ColumnNameRecord.ColumnNameRecordBuilder aValidColumnNameRecord(int recordId) {
        return ColumnNameRecord.builder()
                .id(recordId)
                .bigInt(100L)
                .text("some text")
                .localDate(LocalDate.of(2024, 12, 31))
                .columnWithDashes("dash-value")
                .columnWithDots("dot.value")
                .columnWithSpaces("space value")
                .columnWithUnderscore("under_score")
                .caseInsensitiveColumnWithDashes("ci-dash-value")
                .ueber("umlaut value");
    }

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ColumnNameRecord {

        @JsonProperty("ID")
        private Integer id;

        @JsonProperty("Text")
        private String text;

        @JsonProperty("localdate")
        private LocalDate localDate;

        @JsonProperty("bigInt")
        private Long bigInt;

        @JsonProperty("column-with-dashes")
        private String columnWithDashes;

        @JsonProperty("column.with.dots")
        private String columnWithDots;

        @JsonProperty("column with spaces")
        private String columnWithSpaces;

        @JsonProperty("column_with_underscores")
        private String columnWithUnderscore;

        @JsonProperty("Case-Insensitive-Column-With-Dashes")
        private String caseInsensitiveColumnWithDashes;

        @JsonProperty("über")
        private String ueber;
    }
}
