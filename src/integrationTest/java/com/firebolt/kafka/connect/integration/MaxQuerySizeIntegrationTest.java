package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.firebolt.kafka.connect.PostProcessingConfig;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test to verify that the maxQuerySize configuration properly splits
 * batches into multiple queries when the query size would exceed the limit.
 */
@Slf4j
public class MaxQuerySizeIntegrationTest extends SchemaBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("max_query_size_test_table");
    private static final String TOPIC_NAME = generateTopicName("max-query-size-test-topic");
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, SimpleRecord> producer;
    private long testStartTime;

    private Map<String, String> connectorConfigOverride;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("max-query-size-test");

        // Set a small maxQuerySize (300 bytes) to force multiple batches
        // With a simple table (id, value), each INSERT is approximately:
        // Template: ~50 bytes, parameters: ~15-20 bytes per record
        // So 300 bytes should allow ~2-3 records per batch
        connectorConfigOverride = new HashMap<>();
        connectorConfigOverride.put("maxQuerySize", "300");

        // Setup table, topic, and schema - but NOT the connector yet
        setupTestResourcesWithoutConnector(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                simpleRecordTableSchema(), jsonSimpleRecordSchema());
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
    void testMaxQuerySizeSplitsBatches() throws Exception {
        producer = initializeJsonProducer();

        // Create 10 records - with maxQuerySize of 300 bytes, this should result in multiple batches
        List<SimpleRecord> testRecords = createTestRecords(10);

        // Publish all messages BEFORE creating the connector
        log.info("Publishing {} records to topic {} before creating connector", testRecords.size(), TOPIC_NAME);
        for (int i = 0; i < testRecords.size(); i++) {
            SimpleRecord record = testRecords.get(i);
            ProducerRecord<String, SimpleRecord> producerRecord = new ProducerRecord<>(
                    TOPIC_NAME, "key-" + i, record);
            producer.send(producerRecord).get();
        }
        producer.flush();

        log.info("Published {} records to topic. Now creating connector to process them all at once", testRecords.size());

        // Record the start time just before creating the connector
        testStartTime = System.currentTimeMillis();

        // Now create the connector - it should process all 10 records at once
        registerJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME, connectorConfigOverride);

        log.info("Connector created, waiting for all records to be inserted into Firebolt");

        // Wait for all records to be inserted
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // Verify records were inserted correctly
        verifyRecords(TABLE_NAME, testRecords);

        // Count the number of INSERT queries executed
        int insertQueryCount = countInsertQueries(TABLE_NAME, testStartTime);
        log.info("Found {} INSERT queries for {} records", insertQueryCount, testRecords.size());

        // With maxQuerySize of 300 bytes and ~10 records, we should have 4 queries
        // Each INSERT statement is approximately 75 bytes (template + parameters)
        // So 300 bytes should allow about 3 records per query because we leave some overhead of 5% in the size
        // With 10 records, we expect 4 queries
        assertEquals(4, insertQueryCount, String.format("Expected 4 INSERT queries with maxQuerySize=300, but found %d", insertQueryCount));
    }

    @Test
    void testMaxQuerySizeSplitsBatchesWithPostProcessingScript() throws Exception {
        // Create a simple post-processing script that doesn't modify data
        // This is just to verify transactions work correctly
        String postProcessingScript = preparePostProcessingScript();
        connectorConfigOverride.put(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG, postProcessingScript);

        producer = initializeJsonProducer();

        // Create 10 records - with maxQuerySize of 300 bytes, this should result in multiple batches
        List<SimpleRecord> testRecords = createTestRecords(10);

        // Publish all messages BEFORE creating the connector
        log.info("Publishing {} records to topic {} before creating connector", testRecords.size(), TOPIC_NAME);
        for (int i = 0; i < testRecords.size(); i++) {
            SimpleRecord record = testRecords.get(i);
            ProducerRecord<String, SimpleRecord> producerRecord = new ProducerRecord<>(
                    TOPIC_NAME, "key-" + i, record);
            producer.send(producerRecord).get();
        }
        producer.flush();

        log.info("Published {} records to topic. Now creating connector to process them all at once", testRecords.size());

        // Record the start time just before creating the connector
        testStartTime = System.currentTimeMillis();

        // Now create the connector - it should process all 10 records at once
        registerJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME, connectorConfigOverride);

        log.info("Connector created, waiting for all records to be inserted into Firebolt");

        // Wait for all records to be inserted
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // Verify records were inserted correctly
        verifyRecords(TABLE_NAME, testRecords);

        // Count the number of INSERT queries executed and verify transaction IDs
        int insertQueryCount = countInsertQueries(TABLE_NAME, testStartTime);
        log.info("Found {} INSERT queries for {} records", insertQueryCount, testRecords.size());

        // With maxQuerySize of 300 bytes and ~10 records, we should have 4 queries
        assertEquals(4, insertQueryCount, String.format("Expected 4 INSERT queries with maxQuerySize=300, but found %d", insertQueryCount));

        // Verify that all INSERT queries in the same transaction have the same transaction_begin_lsn
        // With post-processing scripts, all queries from a single batch should be in one transaction
        verifyTransactionIds(TABLE_NAME, testStartTime);
    }

    /**
     * Prepares a simple post-processing script that doesn't modify data.
     * This is just used to enable transaction mode.
     */
    private String preparePostProcessingScript() {
        // A simple script that does nothing - just to enable transactions
        String script = "SELECT 1;";
        PostProcessingConfig postProcessingConfig = new PostProcessingConfig(
                List.of(
                        new PostProcessingConfig.Mapping(TABLE_NAME, script)
                ));
        try {
            return objectMapper.writeValueAsString(postProcessingConfig);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize post processing config", e);
        }
    }

    /**
     * Verifies that all INSERT queries executed within the time range have the same transaction_begin_lsn.
     * This is important when using post-processing scripts, as all queries in a batch should be in one transaction.
     *
     * @param tableName the name of the table
     * @param startTimeMillis the start time in milliseconds (epoch time)
     */
    private void verifyTransactionIds(String tableName, long startTimeMillis) throws SQLException {
        long endTimeMillis = System.currentTimeMillis();
        
        String sql = String.format(
                "SELECT transaction_begin_lsn " +
                "FROM information_schema.engine_query_history " +
                "WHERE query_text LIKE 'INSERT INTO \"%s\" %%' " +
                "AND status = 'ENDED_SUCCESSFULLY' " +
                "AND start_time >= FROM_UNIXTIME(%d) " +
                "AND end_time <= FROM_UNIXTIME(%d) " +
                "AND transaction_begin_lsn IS NOT NULL " +
                "ORDER BY start_time",
                tableName, startTimeMillis / 1000, endTimeMillis / 1000);

        log.debug("Querying transaction IDs: {}", sql);

        Set<String> transactionIds = new HashSet<>();
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(sql)) {
            while (rs.next()) {
                String transactionId = rs.getString("transaction_begin_lsn");
                if (transactionId != null) {
                    transactionIds.add(transactionId);
                    log.debug("Found INSERT query with transaction_begin_lsn: {}", transactionId);
                }
            }
        }

        log.info("Found {} unique transaction IDs for INSERT queries", transactionIds.size());
        
        // With post-processing scripts, all INSERT queries from a single batch should be in the same transaction
        // Since we're processing all 10 records at once in a single batch, they should all be in one transaction
        // However, if maxQuerySize splits them into multiple queries, they should still be in the same transaction
        assertEquals(1, transactionIds.size(), 
                String.format("Expected all INSERT queries to be in the same transaction, but found %d different transaction IDs: %s", 
                        transactionIds.size(), transactionIds));
    }

    /**
     * Creates test records with sequential IDs and values.
     */
    private List<SimpleRecord> createTestRecords(int count) {
        List<SimpleRecord> records = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            records.add(SimpleRecord.builder()
                    .id(i)
                    .value("value-" + i)
                    .build());
        }
        return records;
    }

    /**
     * Counts the number of INSERT queries executed for a given table within a time range.
     *
     * @param tableName the name of the table
     * @param startTimeMillis the start time in milliseconds (epoch time)
     * @return the number of INSERT queries
     */
    private int countInsertQueries(String tableName, long startTimeMillis) throws SQLException {
        long endTimeMillis = System.currentTimeMillis();
        
        String sql = String.format(
                "SELECT COUNT(*) " +
                "FROM information_schema.engine_query_history " +
                "WHERE query_text LIKE 'INSERT INTO \"%s\" %%' " +
                "AND status = 'ENDED_SUCCESSFULLY' " +
                "AND start_time >= FROM_UNIXTIME(%d) " +
                "AND end_time <= FROM_UNIXTIME(%d)",
                tableName, startTimeMillis / 1000, endTimeMillis / 1000);

        log.debug("Querying INSERT query count: {}", sql);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(sql)) {
            if (rs.next()) {
                int count = rs.getInt(1);
                log.info("Found {} INSERT queries for table '{}' between {} and {}", 
                        count, tableName, startTimeMillis, endTimeMillis);
                return count;
            }
            return 0;
        }
    }
}

