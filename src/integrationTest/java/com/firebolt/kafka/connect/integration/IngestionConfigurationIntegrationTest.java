package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class IngestionConfigurationIntegrationTest extends SchemalessBaseIntegrationTest {

    private static String TABLE_NAME;
    private static String TOPIC_NAME;

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        TABLE_NAME = generateTableName("ingestion_config_table_" + RandomStringUtils.randomNumeric(5));
        TOPIC_NAME = generateTopicName("ingestion-config-topic-" + RandomStringUtils.randomNumeric(5));

        generateUniqueConnectorName("ingestion-config-test-" + RandomStringUtils.randomNumeric(5));
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
    void willOnlyGetTheAmountOfRecordsBetweenMinAndMaxBytes() throws Exception {
        Map<String, String> connectorOverrideProperties = new HashMap<>();

        // Configure consumer to wait up to 5 seconds and aim for payload sizes between 4KB and ~5KB
        connectorOverrideProperties.put("consumer.override.fetch.min.bytes", "4000");
        connectorOverrideProperties.put("consumer.override.fetch.max.bytes", "5000");
        connectorOverrideProperties.put("consumer.override.fetch.max.wait.ms", "5000");

        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, ingestionConfigTableSchema(), connectorOverrideProperties);

        producer = initializeSchemalessJsonProducer();

        final AtomicInteger idCounter = new AtomicInteger(1);
        final AtomicInteger totalSent = new AtomicInteger(0);

        int messagesInABatch = 6;
        int messageSizeInBytes = 1000;

        Thread producerThread = new Thread(() -> {
            long endAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            while (System.currentTimeMillis() < endAt) {
                int startId = idCounter.getAndAdd(messagesInABatch);

                // keep track how many has been published so we can check the count
                int batchPublished = publishMessagesInBatches(startId, messagesInABatch, messageSizeInBytes);
                totalSent.addAndGet(batchPublished);

                sleepForMillis(2000);
            }
        }, "continuous-producer-" + RandomStringUtils.randomNumeric(5));

        producerThread.start();

        // wait for the producer to finish
        producerThread.join();

        int expectedCount = totalSent.get();

        waitForDataInFirebolt(TABLE_NAME, expectedCount);

        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedCount, actualCount, "Expected " + expectedCount + " records but found " + actualCount);

        // 6 messages every 3 seconds. Since a messages is a bit over 1k and the threshold is between 4k and 5k we should only get 4 messages at a time
        verifyBatchResultsCheckMaxRecords(4);
    }

    private void verifyBatchResultsCheckMaxRecords(int maxRecordsInBatch) throws SQLException {
        // allow a small tolerance of 50 ms between timestamps
        int timeToleranceMs = 10;

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"value\", \"ingested_at\" " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {

            // get the first record
            rs.next();

            long batchRecordIngestionTime = rs.getTimestamp("ingested_at").toInstant().toEpochMilli();
            int batchSize = 1;
            int recordId = 2;
            while (rs.next()) {
                long currentRecordIngestionTime = rs.getTimestamp("ingested_at").toInstant().toEpochMilli();

                // allow a variation of up to 100ms between records ingested in the same batch
                if (currentRecordIngestionTime - batchRecordIngestionTime < timeToleranceMs) {
                    batchSize++;
                } else {
                    // this means start of a new batch. but first check if the previous batch did not exceed the capacity
                    assertTrue(batchSize <= maxRecordsInBatch, "Expected less than " + maxRecordsInBatch + " records in a batch, but got " + batchSize + " for record " + recordId);

                    // reset the batch size to just this record
                    batchSize = 1;

                    batchRecordIngestionTime = currentRecordIngestionTime;
                }

                recordId++;
            }

            // verify the last batch
            assertTrue(batchSize <= maxRecordsInBatch, "Expected less than 200 records in a batch, but got " + batchSize);

        }
    }

    @Test
    void willWaitForFetchTimeIfThePayloadDoesNotMatchTheMinFetchBytes() throws Exception {
        Map<String, String> connectorOverrideProperties = new HashMap<>();

        // Configure consumer to wait up to 5 seconds and aim for payload sizes between ~300KB and ~400KB
        connectorOverrideProperties.put("consumer.override.fetch.min.bytes", "300000");
        connectorOverrideProperties.put("consumer.override.fetch.max.bytes", "400000");
        connectorOverrideProperties.put("consumer.override.fetch.max.wait.ms", "5000");

        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, ingestionConfigTableSchema(), connectorOverrideProperties);

        producer = initializeSchemalessJsonProducer();

        final AtomicInteger idCounter = new AtomicInteger(1);
        final AtomicInteger totalSent = new AtomicInteger(0);

        int messagesInABatch = 100;
        int messageSizeInBytes = 100;

        Thread producerThread = new Thread(() -> {
            long endAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            while (System.currentTimeMillis() < endAt) {
                int startId = idCounter.getAndAdd(messagesInABatch);

                // keep track how many has been published so we can check the count
                int batchPublished = publishMessagesInBatches(startId, messagesInABatch, messageSizeInBytes);
                totalSent.addAndGet(batchPublished);

                sleepForMillis(3000);
            }
        }, "continuous-producer-" + RandomStringUtils.randomNumeric(5));

        producerThread.start();

        // wait for the producer to finish
        producerThread.join();

        int expectedCount = totalSent.get();

        waitForDataInFirebolt(TABLE_NAME, expectedCount);

        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedCount, actualCount, "Expected " + expectedCount + " records but found " + actualCount);

        // we are pushing 100 messages every 3 seconds and consuming every 5 seconds. So there should be no more than 200 messages in one batch and at least 5 seconds in between batches
        verifyBatchResultsCheckTimeBetweenBatches(200, TimeUnit.SECONDS.toMillis(5));
    }

    private void verifyBatchResultsCheckTimeBetweenBatches(int maxRecordsInBatch, long minDurationBetweenBatches) throws SQLException {
        // allow a small tolerance between timestamps from the same batch
        int timeToleranceMs = 10;

        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"id\", \"value\", \"ingested_at\" " +
                        "FROM \"%s\" ORDER BY \"id\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {

            // get the first record
            rs.next();

            long batchRecordIngestionTime = rs.getTimestamp("ingested_at").toInstant().toEpochMilli();
            int batchSize = 1;
            int recordId = 2;
            while (rs.next()) {
                long currentRecordIngestionTime = rs.getTimestamp("ingested_at").toInstant().toEpochMilli();

                // allow a variation of up to 100ms between records ingested in the same batch
                if (currentRecordIngestionTime - batchRecordIngestionTime < timeToleranceMs) {
                    batchSize++;
                } else {
                    // this means start of a new batch. but first check if the previous batch did not exceed the capacity
                    assertTrue(batchSize <= maxRecordsInBatch, "Expected less than " + maxRecordsInBatch + " records in a batch, but got " + batchSize + " for record " + recordId);

                    // reset the batch size to just this record
                    batchSize = 1;

                    // make sure the difference between the two batches is at least minDurationBetweenBatches
                    assertTrue(currentRecordIngestionTime - batchRecordIngestionTime >= minDurationBetweenBatches - timeToleranceMs ,
                            "Expected to have a minimnum duration between batches of " + (minDurationBetweenBatches - timeToleranceMs) + " but was " + (currentRecordIngestionTime - batchRecordIngestionTime + " for record " + recordId));

                    batchRecordIngestionTime = currentRecordIngestionTime;
                }

                recordId++;
            }

            // verify the last batch
            assertTrue(batchSize <= maxRecordsInBatch, "Expected less than 200 records in a batch, but got " + batchSize);

        }
    }

    private int publishMessagesInBatches(int startId, int nrOfMessages, int size) {
        List<SimpleRecord> batch = new ArrayList<>(nrOfMessages);
        for (int i = 0; i < nrOfMessages; i++) {
            batch.add(aValidTestRecord(startId + i, RandomStringUtils.randomAlphanumeric(size)));
        }
        try {
            publishMessagesInBatches(batch);
            return batch.size();
        } catch (Exception e) {
            log.error("Failed to publish periodic batch: {}", e.getMessage());
            return 0;
        }
    }

    private Supplier<String> ingestionConfigTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"value\" TEXT NOT NULL," +
                "\"ingested_at\" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP )";
    }

    private void publishMessagesInBatches(List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "post-processing-script-test-key-" + record.getId();
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(TOPIC_NAME, key, objectMapper.writeValueAsString(record));

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message with key {}: {}", key, exception.getMessage());
                } else {
                    log.info("Successfully sent message: key={} valueSize={}B keySize={}B partition={} offset={}",
                            key, metadata.serializedValueSize(), metadata.serializedKeySize(),
                            metadata.partition(), metadata.offset());
                }
            }).get();
        }

        producer.flush();
    }

    private SimpleRecord aValidTestRecord(int recordId, String value) {
        return SimpleRecord.builder()
                .id(recordId)
                .value(value)
                .build();
    }

}
