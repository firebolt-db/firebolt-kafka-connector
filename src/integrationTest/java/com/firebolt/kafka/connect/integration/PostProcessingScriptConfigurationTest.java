package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.firebolt.kafka.connect.PostProcessingConfig;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class PostProcessingScriptConfigurationTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("post_processing_table");
    private static final String TOPIC_NAME = generateTopicName("post-processing-topic");

    private static final String TARGET_TABLE_NAME = generateTableName("target_table_post_processing");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("post-processing-test");

        // setup target table
        String createTargetTableSql = String.format(targetTableSchema().get(), TARGET_TABLE_NAME);
        try {
            fireboltDefaultDbClient.executeUpdate(createTargetTableSql);
        } catch (SQLException e) {
            fail("Cannot create the target table");
        }

        Map<String, String> connectorOverrideProperties = new HashMap<>();
        connectorOverrideProperties.put(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG, preparePostProcessingScript());

        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, postProcessingTableSchema(), connectorOverrideProperties);
    }

    private String preparePostProcessingScript() {
        // the script will concatenate the id and upper case the text and will create just one column
        String script =
                "INSERT INTO %s (processed) \n" +
                "SELECT id::TEXT || UPPER(comment)\n" +
                "FROM %s where batch_id=\'${firebolt_param.batch_id}\';";
        PostProcessingConfig postProcessingConfig = new PostProcessingConfig(
                List.of(
                        new PostProcessingConfig.Mapping(TABLE_NAME, String.format(script, TARGET_TABLE_NAME, TABLE_NAME))
                ));
        try {
            return objectMapper.writeValueAsString(postProcessingConfig);
        } catch (JsonProcessingException e) {
            fail("Failed to serialized the post processing config");
            return null;
        }
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }

        safelyDropTable(TARGET_TABLE_NAME);

        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);

        super.tearDown();
    }

    @Test
    void testPostProcessingScript() throws Exception {
        producer = initializeSchemalessJsonProducer();

        List<SimpleRecord> testRecords = Arrays.asList(
                aValidTestRecord(1, "my comment1"),
                aValidTestRecord(2, "my comment2"),
                aValidTestRecord(3, "my comment3")
        );

        // publish the messages to kafka topic
        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // check that all the records have the expected value
        verifyPostProcessingScript(testRecords);

        // check that all the values are in the target table
        verifyTargetTableResults(testRecords);
    }

    private Supplier<String> postProcessingTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"comment\" TEXT NOT NULL, " +
                "\"batch_id\" TEXT NOT NULL )";
    }

    private Supplier<String> targetTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"processed\" TEXT NOT NULL)";
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

    private void verifyTargetTableResults(List<SimpleRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TARGET_TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"processed\" " +
                        "FROM \"%s\" ORDER BY \"processed\" ASC", TARGET_TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                SimpleRecord expected = expectedRecords.get(recordIndex);
                String expectedValue = expected.getId() + expected.getComment().toUpperCase();

                assertEquals(expectedValue, rs.getString("processed"));
                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    private void verifyPostProcessingScript(List<SimpleRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"comment\", \"batch_id\" " +
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
                assertNotNull(rs.getString("batch_id"));
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
