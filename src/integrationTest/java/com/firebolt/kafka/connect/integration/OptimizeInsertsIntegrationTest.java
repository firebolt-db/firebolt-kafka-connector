package com.firebolt.kafka.connect.integration;

import java.sql.ResultSet;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
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
public class OptimizeInsertsIntegrationTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("optimize_inserts_table");
    private static final String TOPIC_NAME = generateTopicName("optimize-inserts-topic");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("optimize-inserts-test");
        
        // Setup test resources with optimize.inserts=true
        Map<String, String> connectorOverride = new HashMap<>();
        connectorOverride.put("optimize.inserts", "true");
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, simpleRecordTableSchema(), connectorOverride);
        
        // Initialize producer
        producer = initializeSchemalessJsonProducer();
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
    void testOptimizeInsertsCreatesSingleInsertPerQuery() throws Exception {
        // Create 10 test records
        List<SimpleRecord> testRecords = Arrays.asList(
                new SimpleRecord(1, "Record1"),
                new SimpleRecord(2, "Record2"),
                new SimpleRecord(3, "Record3"),
                new SimpleRecord(4, "Record4"),
                new SimpleRecord(5, "Record5"),
                new SimpleRecord(6, "Record6"),
                new SimpleRecord(7, "Record7"),
                new SimpleRecord(8, "Record8"),
                new SimpleRecord(9, "Record9"),
                new SimpleRecord(10, "Record10")
        );

        // Record start time before publishing messages
        Instant startTime = Instant.now();

        // Publish messages to Kafka topic
        log.info("Publishing {} messages to topic {}", testRecords.size(), TOPIC_NAME);
        publishMessages(testRecords);

        // Wait for all records to be inserted into Firebolt
        log.info("Waiting for {} records to be inserted into Firebolt", testRecords.size());
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        // Wait a bit more to ensure query history is updated
        sleepForMillis(TimeUnit.SECONDS.toMillis(10));

        // Query the query history to get all INSERT queries executed after start time
        log.info("Querying query history for INSERT queries after {}", startTime);
        String queryHistorySql = String.format(
                "SELECT query_text, start_time, end_time " +
                        "FROM information_schema.engine_query_history " +
                        "WHERE start_time >= '%s' " +
                        "  AND status = 'ENDED_SUCCESSFULLY' " +
                        "  AND query_text LIKE 'INSERT INTO \"%s\"%%' " +
                        "  ORDER BY start_time ASC;",
                startTime, TABLE_NAME);
        ResultSet resultSet = fireboltDefaultDbClient.executeQuery(queryHistorySql);

        int queryCount = 0;
        Pattern insertPattern = Pattern.compile("(?i)INSERT\\s+INTO", Pattern.CASE_INSENSITIVE);

        while (resultSet.next()) {
            queryCount++;
            String queryText = resultSet.getString("query_text");
            String startTimeStr = resultSet.getString("start_time");
            String endTimeStr = resultSet.getString("end_time");

            log.info("Query #{} - Start: {}, End: {}", queryCount, startTimeStr, endTimeStr);
            log.info("Query text: {}", queryText);

            // Count the number of INSERT INTO statements in the query
            long insertCount = insertPattern.matcher(queryText).results().count();

            log.info("Query #{} contains {} INSERT INTO statement(s)", queryCount, insertCount);
            assertEquals(1, insertCount,
                    String.format("Query #%d should contain exactly one INSERT INTO statement, but found %d. Query: %s",
                            queryCount, insertCount, queryText));
        }

        assertTrue(queryCount > 0, "Expected at least one INSERT query in query history, but found none");
        log.info("Verified {} INSERT queries, all containing exactly one INSERT INTO statement", queryCount);
    }

    private void publishMessages(List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "optimize-inserts-test-key-" + record.getId();
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
        log.info("Published {} messages to topic {}", records.size(), TOPIC_NAME);
    }
}

