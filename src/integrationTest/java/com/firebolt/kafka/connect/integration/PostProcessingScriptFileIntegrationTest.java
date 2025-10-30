package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.firebolt.kafka.connect.PostProcessingConfig;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
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
public class PostProcessingScriptFileIntegrationTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("post_processing_file_table");
    private static final String TOPIC_NAME = generateTopicName("post-processing-file-topic");

    private static final String TARGET_TABLE_NAME = generateTableName("target_table_post_processing_file");

    private Producer<String, String> producer;
    private Path scriptFile;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("post-processing-file-test");

        // setup target table
        String createTargetTableSql = String.format(targetTableSchema().get(), TARGET_TABLE_NAME);
        try {
            fireboltDefaultDbClient.executeUpdate(createTargetTableSql);
        } catch (SQLException e) {
            fail("Cannot create the target table");
        }

        // Create the SQL script file
        scriptFile = createScriptFile();

        Map<String, String> connectorOverrideProperties = new HashMap<>();
        connectorOverrideProperties.put(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG, preparePostProcessingScript());

        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, postProcessingTableSchema(), connectorOverrideProperties);
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }

        // Clean up the script file if it was created
        // Delete from all directories where it was copied
        if (scriptFile != null) {
            try {
                // Convert container path back to host path
                // Container path: /etc/kafka-connect/scripts/filename.sql
                // Delete the file from all possible scripts directories
                if (scriptFile.toString().startsWith("/etc/kafka-connect/scripts")) {
                    String fileName = scriptFile.getFileName().toString();
                    Path[] possibleScriptsDirs = {
                        Paths.get("src/integrationTest/docker/kafka-connect-cloud/scripts"),
                        Paths.get("src/integrationTest/docker/kafka-connect-3.9.1/scripts"),
                        Paths.get("src/integrationTest/docker/kafka-connect-4.0/scripts")
                    };
                    
                    for (Path scriptsDir : possibleScriptsDirs) {
                        Path hostFile = scriptsDir.resolve(fileName);
                        if (Files.exists(hostFile)) {
                            Files.delete(hostFile);
                            log.info("Deleted script file: {}", hostFile);
                        }
                    }
                }
            } catch (IOException e) {
                log.warn("Failed to delete script file: {}", e.getMessage());
            }
        }

        safelyDropTable(TARGET_TABLE_NAME);

        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);

        super.tearDown();
    }

    /**
     * Creates a temporary SQL script file that will be used for post-processing.
     * The file is created in a location accessible to the Kafka Connect Docker container.
     * All docker-compose setups mount ./scripts at /etc/kafka-connect/scripts
     * The file is copied into all possible script directories to ensure it's available
     * regardless of which Kafka Connect setup is being used.
     * 
     * @return Path to the created script file (as it should be accessed from within the container)
     */
    private Path createScriptFile() {
        try {
            // Copy the script file into all possible script directories
            Path[] possibleScriptsDirs = {
                Paths.get("src/integrationTest/docker/kafka-connect-cloud/scripts"),
                Paths.get("src/integrationTest/docker/kafka-connect-3.9.1/scripts"),
                Paths.get("src/integrationTest/docker/kafka-connect-4.0/scripts")
            };

            // Create a unique script file name to avoid conflicts between test runs
            String scriptFileName = "post_processing_script_" + System.currentTimeMillis() + "_" + 
                                    Thread.currentThread().getId() + ".sql";

            // Write the SQL script content
            String scriptContent = String.format(
                    "INSERT INTO \"%s\" (processed) \n" +
                    "SELECT id::TEXT || UPPER(value)\n" +
                    "FROM \"%s\" where batch_id='${firebolt_param.batch_id}';",
                    TARGET_TABLE_NAME, TABLE_NAME);

            // Copy the script file into all directories
            for (Path scriptsDir : possibleScriptsDirs) {
                // Ensure the directory exists
                if (!Files.exists(scriptsDir)) {
                    Files.createDirectories(scriptsDir);
                    log.info("Created scripts directory: {}", scriptsDir.toAbsolutePath());
                }

                Path hostScriptFile = scriptsDir.resolve(scriptFileName);
                Files.writeString(hostScriptFile, scriptContent);
                log.info("Created post-processing script file on host: {}", hostScriptFile.toAbsolutePath());
            }

            // Return the path as it should be accessed from within the Docker container
            // All docker-compose setups mount ./scripts at /etc/kafka-connect/scripts
            Path containerPath = Paths.get("/etc/kafka-connect/scripts", scriptFileName);
            log.info("Script file path inside container will be: {} (copied to all script directories)", containerPath);
            
            return containerPath;
        } catch (IOException e) {
            log.error("Failed to create script file", e);
            fail("Failed to create script file: " + e.getMessage());
            return null;
        }
    }

    private String preparePostProcessingScript() {
        PostProcessingConfig postProcessingConfig = new PostProcessingConfig(
                List.of(
                        new PostProcessingConfig.Mapping(TABLE_NAME, null, scriptFile.toString())
                ));
        try {
            return objectMapper.writeValueAsString(postProcessingConfig);
        } catch (JsonProcessingException e) {
            fail("Failed to serialize the post processing config");
            return null;
        }
    }

    @Test
    void testPostProcessingScriptFromFile() throws Exception {
        producer = initializeSchemalessJsonProducer();

        List<SimpleRecord> testRecords = Arrays.asList(
                new SimpleRecord(1, "my comment1"),
                new SimpleRecord(2, "my comment2"),
                new SimpleRecord(3, "my comment3")
        );

        // publish the messages to kafka topic
        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // check that all the records have the expected value
        verifyPostProcessingScript(testRecords);

        // check that all the values are in the target table (validating script file execution)
        verifyTargetTableResults(testRecords);
    }

    private Supplier<String> postProcessingTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"value\" TEXT NOT NULL, " +
                "\"batch_id\" TEXT NOT NULL )";
    }

    private Supplier<String> targetTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"processed\" TEXT NOT NULL)";
    }

    private void publishMessages(List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "post-processing-script-file-test-key-" + record.getId();
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
                String expectedValue = expected.getId() + expected.getValue().toUpperCase();

                assertEquals(expectedValue, rs.getString("processed"),
                        "Processed value doesn't match expected for record " + recordIndex);
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
                "SELECT \"id\", \"value\", \"batch_id\" " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                SimpleRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                assertEquals(expected.getId(), rs.getInt("id"));
                assertEquals(expected.getValue(), rs.getString("value"));
                assertNotNull(rs.getString("batch_id"));
                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
}
