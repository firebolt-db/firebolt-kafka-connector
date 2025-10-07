package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TableNameTest extends BaseIntegrationTest {

    private Producer<String, SimpleRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
    }

    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }

        super.tearDown();
    }

    @ParameterizedTest
    @CsvSource({
            "table-name-with-dashes,name-with-dashes-table,topic1",
            "table-name-with-upperchars,UPPER_CHARS_TABLE_NAME,topic2"
    })
    void testTableNameWithDashes(String connectorName, String tableName, String topicName) throws Exception {
        String schemaSubject = topicName + "-value";
        try {
           // Generate unique connector name for this test run
           generateUniqueConnectorName(connectorName);

           // Setup test resources using centralized method

           setupTestResources(topicName, tableName, schemaSubject,
                   simpleRecordTableSchema(), jsonSimpleRecordSchema());

           producer = initializeJsonProducer();

           List<SimpleRecord> testRecords = createTestRecords();

           // publish the messages to kafka topic
           publishMessages(topicName, testRecords);

           waitForDataInFirebolt(tableName, testRecords.size());

           // check that all the records have the expected value
           verifyRecords(tableName, testRecords);
       } finally {
           // Clean up test resources
           cleanupTestResources(tableName, topicName, schemaSubject);
       }
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<SimpleRecord> createTestRecords() {
        return Arrays.asList(
                aValidTestRecord(1),
                aValidTestRecord(2),
                aValidTestRecord(3)
        );
    }

    private void publishMessages(String topicName, List<SimpleRecord> records) throws Exception {
        for (SimpleRecord record : records) {
            String key = "column-name-casing-test-key-" + record.getId();
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

    // Use BaseIntegrationTest.verifyRecords(String, List<SimpleRecord>)

    private SimpleRecord aValidTestRecord(int recordId) {
        return SimpleRecord.builder()
                .id(recordId)
                .value("record : " + recordId)
                .build();

    }
}
