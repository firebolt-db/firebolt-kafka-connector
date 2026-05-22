package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.STRESS)
public class LargePayloadTest extends SchemaBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("large_payload_test_table");
    private String TOPIC_NAME = generateTopicName("large-record-test-topic");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, SimpleRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("large-record-test");

        // poll 100 records so the whole payload will exceed the size of the request to firebolt
        Map<String, String> connectionDefinitionOverride = Map.of(
                "consumer.override.max.poll.records", "100",
                "consumer.override.max.partition.fetch.bytes", "50000000",
                "consumer.override.fetch.max.bytes", "50000000");

        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                simpleRecordTableSchema(), jsonSimpleRecordSchema(), connectionDefinitionOverride);
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
    void largePreparedStatementBatchDoesNotBreakTestHarness() throws Exception {
        producer = initializeJsonProducer();

        // "Ā" takes 2 bytes. We repeat it 500k times so it will be approx 1MB.
        // Since some accounts on cloud have the max 40MB cloud limit, publish 45 messages so we make sure that even on cloud it will exceed capacity
        List<SimpleRecord> testRecords = new ArrayList<>();
        for (int i = 0; i < 45; i++) {
            testRecords.add(aValidTestRecord(1, "Ā".repeat(500000)));
        }

        publishMessages(TOPIC_NAME, testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size(), Duration.ofMinutes(3));

        verifyRecords(TABLE_NAME, testRecords);
    }

    /**
     * Our automation CI account is configured with 40bm max payload. The io.confluent.connect.json.JsonSchemaConverter cannot deserialize messages larger than 20MB. So cannot really test this on CI
     * We can test it locally with a different account
     * @throws Exception
     */
    @Test
    @Disabled
    void willNotProcessSingleLargeMessageThatExceedsMaxLimit() throws Exception {
        producer = initializeJsonProducer();

        // "Ā" takes 2 bytes. We repeat it 500k times so it will be approx 1MB.
        // Since some accounts on cloud have the max 40MB cloud limit, publish 45 messages so we make sure that even on cloud it will exceed capacity
        SimpleRecord recordShouldNotBeSaved = aValidTestRecord(1, "Ā".repeat(2100000)); // 42 mb should never be processed
        SimpleRecord recordShouldBeSaved = aValidTestRecord(1, "Ā".repeat(10000)); // this should be processed
        List<SimpleRecord> testRecords = Arrays.asList(recordShouldNotBeSaved, recordShouldBeSaved);

        publishMessages(TOPIC_NAME, testRecords);

        waitForDataInFirebolt(TABLE_NAME, 1);

        verifyRecords(TABLE_NAME, List.of(recordShouldBeSaved));
    }

    private void publishMessages(String topicName, List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "large-message-test-key-" + record.getId();
            ProducerRecord<String, SimpleRecord> producerRecord =
                    new ProducerRecord<>(topicName, key, record);

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

    private SimpleRecord aValidTestRecord(int recordId, String value) {
        return SimpleRecord.builder()
                .id(recordId)
                .value(value)
                .build();

    }
}
