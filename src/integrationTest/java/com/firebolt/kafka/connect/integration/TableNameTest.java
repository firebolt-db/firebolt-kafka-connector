package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TableNameTest extends SchemalessBaseIntegrationTest {

    private Producer<String, String> producer;

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
    @MethodSource("tableNames")
    void testTableNameWithDashes(String connectorName, String tableName, String topicName, Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running test {}", description);

        try {
           // Generate unique connector name for this test run
           generateUniqueConnectorName(connectorName + "-" +connectorOverride.get("ingestion.type"));

           // Setup test resources using centralized method
           setupSchemalessTestResources(topicName, tableName, simpleRecordTableSchema(), connectorOverride);

           producer = initializeSchemalessJsonProducer();

           List<SimpleRecord> testRecords = createTestRecords();

           // publish the messages to kafka topic
           publishMessages(topicName, testRecords);

           waitForDataInFirebolt(tableName, testRecords.size());

           // check that all the records have the expected value
           verifyRecords(tableName, testRecords);
       } finally {
           // Clean up test resources
           cleanupSchemalessTestResources(tableName, topicName);
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
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, key, objectMapper.writeValueAsString(record));

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

    // When we have a way to run firebolt-core with the image that has the fix we can uncomment and use sqlAndBinaryTestSetupWithOrWithoutNulls
    // until then we will run these tests locally against core
    protected static Stream<Arguments> tableNames() {
        return Stream.of(
                Arguments.of("connector-table-name-with-dashes", "name-with-dashes-table", "topic1", Map.of("ingestion.type", "sql"), "sql ingestion with for table name with dashes in name"),
//                Arguments.of("connector-table-name-with-dashes", "name-with-dashes-table", "topic1", Map.of("ingestion.type", "binary"), "binary ingestion with for table name with dashes in name"),
                Arguments.of("connector-table-name-with-uppercase", "UPPER_CHARS_TABLE_NAME", "topic2", Map.of("ingestion.type", "sql"), "sql ingestion with for table name with upper case in name")
//                Arguments.of("connector-table-name-with-uppercase", "UPPER_CHARS_TABLE_NAME", "topic2", Map.of("ingestion.type", "binary"), "binary ingestion with for table name upper case in name")
        );
    }
}
