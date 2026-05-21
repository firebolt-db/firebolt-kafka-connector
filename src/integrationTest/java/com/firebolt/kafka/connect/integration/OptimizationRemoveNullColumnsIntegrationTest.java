package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.utils.TestTag;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import static org.awaitility.Awaitility.await;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.CONNECTOR)
public class OptimizationRemoveNullColumnsIntegrationTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("optimization_remove_null_columns_table");
    private static final String TOPIC_NAME = generateTopicName("optimization-remove-null-columns-topic");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("optimization-remove-null-columns-test");
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
    void testColumnRemovalIfAllValuesAreNullInBatch() throws Exception {
        // Create the test table with the provided schema
        createTable(sparseTableSchema(), TABLE_NAME);

        // Create Kafka topic
        log.info("Creating Kafka topic: {}", TOPIC_NAME);
        createKafkaTopic(TOPIC_NAME);

        producer = initializeSchemalessJsonProducer();

        List<RecordWithNullColumns> testRecords = Arrays.asList(
                aValidTestRecord(1).col1("col1").build(),
                aValidTestRecord(2).col2("col2").build(),
                aValidTestRecord(3).col3("col3").build(),
                aValidTestRecord(4).col4("col4").build(),
                aValidTestRecord(5).col5("col5").build(),
                aValidTestRecord(6).col6("col6").build(),
                aValidTestRecord(7).col7("col7").build(),
                aValidTestRecord(8).col8("col8").build(),
                aValidTestRecord(9).col9("col9").build(),
                aValidTestRecord(10).col10("col10").build()
        );

        Instant startTime = Instant.now();

        // publish the messages to kafka topic
        publishMessages(testRecords);

        registerSchemalessJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME, Collections.emptyMap());

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // check that all the values are in the target table
        verifyResults(testRecords);

        String queryHistorySql = "SELECT query_text, start_time " +
                "FROM information_schema.engine_query_history " +
                "WHERE start_time > '" + startTime + "'" +
                "  AND status ='ENDED_SUCCESSFULLY' " +
                "  AND query_text LIKE 'INSERT INTO%'" +
                "  ORDER by start_time asc";

        // Query history may lag behind data flush — poll until the INSERT appears
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> {
                try (ResultSet rs = fireboltDefaultDbClient.executeQuery(queryHistorySql)) {
                    return rs.next();
                }
            });

        ResultSet resultSet = fireboltDefaultDbClient.executeQuery(queryHistorySql);
        assertTrue(resultSet.next());

        String insertQuery = resultSet.getString("query_text");
        log.info("Verifying query: {}", insertQuery);
        Set<String> columnNamesFromInsertStatement = getColumnNameFromInsertStatement(insertQuery);

        // none of these columns should be present
        assertFalse(columnNamesFromInsertStatement.contains("col11"));
        assertFalse(columnNamesFromInsertStatement.contains("col12"));
        assertFalse(columnNamesFromInsertStatement.contains("col13"));
        assertFalse(columnNamesFromInsertStatement.contains("col14"));
        assertFalse(columnNamesFromInsertStatement.contains("col15"));
        assertFalse(columnNamesFromInsertStatement.contains("col16"));

        // at least one of these columns should be present
        assertTrue(columnNamesFromInsertStatement.contains("id"));

        Set<String> expected = Set.of("id", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10");

        assertEquals(expected, columnNamesFromInsertStatement);
    }

    private Set<String> getColumnNameFromInsertStatement(String insertStatement) {
       List<Set<String>> columnNamesFromMultiInsertStatements = getColumnNamesFromMultiInsertStatements(insertStatement);

       if (columnNamesFromMultiInsertStatements.size() == 1) {
           return columnNamesFromMultiInsertStatements.get(0);
       }

       // for the same multi insert statement the columns should be the same
       Set<String> firstInsertStatementColumnNames = columnNamesFromMultiInsertStatements.get(0);
       columnNamesFromMultiInsertStatements.stream()
               .forEach(columnSet -> Assertions.assertEquals(firstInsertStatementColumnNames, columnSet));

       return firstInsertStatementColumnNames;
    }

    private List<Set<String>> getColumnNamesFromMultiInsertStatements(String multi) {
        if (multi == null || multi.isBlank()) return java.util.Collections.emptyList();
        Pattern p = Pattern.compile("(?i)INSERT\\s+INTO\\s+\"[^\"]+\"\\s*\\(([^)]*)\\)");
        List<Set<String>> result = new java.util.ArrayList<>();
        Matcher m = p.matcher(multi);
        while (m.find()) {
            String cols = m.group(1);
            String[] parts = cols.split(",");
            java.util.Set<String> names = new java.util.LinkedHashSet<>();
            for (String part : parts) {
                String name = part.trim();
                if (name.startsWith("\"") && name.endsWith("\"") && name.length() >= 2) {
                    name = name.substring(1, name.length() - 1);
                }
                names.add(name);
            }
            result.add(names);
        }
        return result;
    }

    private Supplier<String> sparseTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"col1\" TEXT NULL, " +
                "\"col2\" TEXT NULL, " +
                "\"col3\" TEXT NULL, " +
                "\"col4\" TEXT NULL, " +
                "\"col5\" TEXT NULL, " +
                "\"col6\" TEXT NULL, " +
                "\"col7\" TEXT NULL, " +
                "\"col8\" TEXT NULL, " +
                "\"col9\" TEXT NULL, " +
                "\"col10\" TEXT NULL, " +
                "\"col11\" TEXT NULL, " +
                "\"col12\" TEXT NULL, " +
                "\"col13\" TEXT NULL, " +
                "\"col14\" TEXT NULL, " +
                "\"col15\" TEXT NULL, " +
                "\"col16\" TEXT NULL " +  // this will never be present in the record so should never be part of the insert
                ")";
    }

    private void publishMessages(List<RecordWithNullColumns> records) throws Exception {
        for (RecordWithNullColumns record : records) {
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

    private void verifyResults(List<RecordWithNullColumns> expectedRecords) throws SQLException {
        // Verify specific records by id
        String selectQuery = String.format(
                "SELECT \"id\", \"col1\", \"col2\", \"col3\", \"col4\", \"col5\", \"col6\", \"col7\", \"col8\", \"col9\", " +
                        "\"col10\", \"col11\", \"col12\", \"col13\", \"col14\", \"col15\",  \"col16\", " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                RecordWithNullColumns expected = expectedRecords.get(recordIndex);
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getCol1(), rs.getString("col1"));
                assertEquals(expected.getCol2(), rs.getString("col2"));
                assertEquals(expected.getCol3(), rs.getString("col3"));
                assertEquals(expected.getCol4(), rs.getString("col4"));
                assertEquals(expected.getCol5(), rs.getString("col5"));
                assertEquals(expected.getCol6(), rs.getString("col6"));
                assertEquals(expected.getCol7(), rs.getString("col7"));
                assertEquals(expected.getCol8(), rs.getString("col8"));
                assertEquals(expected.getCol9(), rs.getString("col9"));
                assertEquals(expected.getCol10(), rs.getString("col10"));
                // col11 to col15 are always with null values
                assertNull(rs.getString("col11"));
                assertNull(rs.getString("col12"));
                assertNull(rs.getString("col13"));
                assertNull(rs.getString("col14"));
                assertNull(rs.getString("col15"));

                // col16 is not even present on the messages as an attribute
                assertNull(rs.getString("col16"));

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private RecordWithNullColumns.RecordWithNullColumnsBuilder aValidTestRecord(int recordId) {
        return RecordWithNullColumns.builder()
                .id(recordId);
    }

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class RecordWithNullColumns {

        private Integer id;

        private String col1;
        private String col2;
        private String col3;
        private String col4;
        private String col5;
        private String col6;
        private String col7;
        private String col8;
        private String col9;
        private String col10;
        private String col11;
        private String col12;
        private String col13;
        private String col14;
        private String col15;
    }

}