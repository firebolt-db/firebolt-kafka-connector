package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import com.firebolt.kafka.connect.utils.TopicOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.CLOUD)
public class ExactlyOnceSchemaIntegrationTest extends SchemaBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("exactly_once_schema_table");
    private static final String TOPIC_NAME = generateTopicName("exactly-once-schema-topic");
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";
    private static final Integer PARTITION_0 = 0;
    private static final Integer PARTITION_1 = 1;

    private Producer<String, SimpleRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("exactly-once-schema");

        // Create table, topic, schema, and register connector with exactlyOnce=true
        Map<String, String> overrides = new HashMap<>();
        overrides.put("exactlyOnce", "true");
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                simpleRecordTableSchema(), jsonSimpleRecordSchema(), overrides, TopicOptions.builder().partitions(2).build());
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(1));
        }
        cleanupTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        removeMetadataTable();
        super.tearDown();
    }

    @Test
    void updatesMetadataAndIngestsRows() throws Exception {
        producer = initializeJsonProducer();
        Map<Integer, Long> initialMeta = readMetadataOffsets(TOPIC_NAME);
        assertTrue(initialMeta.isEmpty(), "Metadata offsets should be empty initially");

        // Create test records
        SimpleRecord record1 = new SimpleRecord(1, "value1");
        SimpleRecord record2 = new SimpleRecord(2, "value2");
        
        // Send records to different partitions
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + record1.getId(), record1));
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_1, "key" + record2.getId(), record2));
        producer.flush();

        waitForDataInFirebolt(TABLE_NAME, 2);

        // verify metadata offsets: at least 0 for both partitions
        Map<Integer, Long> meta = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, meta.size());
        assertEquals(0L, meta.get(0));
        assertEquals(0L, meta.get(1));
    }

    @Test
    void restartPreventsDuplicateProcessing() throws Exception {
        producer = initializeJsonProducer();
        
        // Create test records
        SimpleRecord record1 = new SimpleRecord(10, "duplicate_test_1");
        SimpleRecord record2 = new SimpleRecord(11, "duplicate_test_2");
        SimpleRecord record3 = new SimpleRecord(12, "duplicate_test_3");

        // Send 2 records on partition 0
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + record1.getId(), record1));
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + record2.getId(), record2));
        producer.flush();

        waitForDataInFirebolt(TABLE_NAME, 2);

        Map<Integer, Long> metadataOffsets1 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets1.size());
        assertEquals(1L, metadataOffsets1.get(0));
        assertEquals(-1L, metadataOffsets1.get(1));

        // reset offsets
        stopConnectorResetOffsetsAndRestartConnector();

        // validate offsets are preserved in metadata table
        Map<Integer, Long> metadataOffsets2 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets2.size());
        assertEquals(1L, metadataOffsets2.get(0));
        assertEquals(-1L, metadataOffsets2.get(1));

        // wait a bit and validate no duplicates
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + record3.getId(), record3));
        waitForDataInFirebolt(TABLE_NAME, 3);

        Map<Integer, Long> metadataOffsets3 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets3.size());
        assertEquals(2L, metadataOffsets3.get(0));
        assertEquals(-1L, metadataOffsets3.get(1));
    }

    @Test
    void perPartitionOffsetsOnlyNewProcessed() throws Exception {
        producer = initializeJsonProducer();
        
        SimpleRecord record1 = new SimpleRecord(100, "partition_0_record");
        SimpleRecord record2 = new SimpleRecord(200, "partition_1_record");
        
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + record1.getId(), record1));
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_1, "key" + record2.getId(), record2));
        producer.flush();
        
        waitForDataInFirebolt(TABLE_NAME, 2);

        Map<Integer, Long> metadataOffsets1 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets1.size());
        assertEquals(0L, metadataOffsets1.get(0));
        assertEquals(0L, metadataOffsets1.get(1));

        // send new data only to partition 1
        SimpleRecord record3 = new SimpleRecord(201, "partition_1_new_record");
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_1, "key" + record3.getId(), record3)); // new
        producer.flush();
        
        waitForDataInFirebolt(TABLE_NAME, 3);

        Map<Integer, Long> metadataOffsets2 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets2.size());
        assertEquals(0L, metadataOffsets2.get(0));
        assertEquals(1L, metadataOffsets2.get(1));
    }

    @Test
    void multipleRestartsAndOffsetResetsPreservesExactlyOnceSemantics() throws Exception {
        producer = initializeJsonProducer();
        
        // Initial data
        SimpleRecord initialRecord = new SimpleRecord(300, "initial_record");
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + initialRecord.getId(), initialRecord));
        producer.flush();
        
        waitForDataInFirebolt(TABLE_NAME, 1);
        assertEquals(0L, readMetadataOffsets(TOPIC_NAME).get(0));

        // First restart
        stopConnectorResetOffsetsAndRestartConnector();

        // Send new data
        SimpleRecord newRecord = new SimpleRecord(301, "new_record");
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + newRecord.getId(), newRecord));
        producer.flush();
        
        waitForDataInFirebolt(TABLE_NAME, 2);
        assertEquals(1L, readMetadataOffsets(TOPIC_NAME).get(0));

        // Second restart
        stopConnectorResetOffsetsAndRestartConnector();

        // Send truly new data
        SimpleRecord finalRecord = new SimpleRecord(302, "final_record");
        producer.send(new ProducerRecord<>(TOPIC_NAME, PARTITION_0, "key" + finalRecord.getId(), finalRecord));
        producer.flush();
        
        waitForDataInFirebolt(TABLE_NAME, 3);
        assertEquals(2L, readMetadataOffsets(TOPIC_NAME).get(0));
    }

    private void stopConnectorResetOffsetsAndRestartConnector() throws IOException {
        kafkaConnectClient.stopConnector(testConnectorName);
        kafkaConnectClient.waitForConnectorStopped(testConnectorName, Duration.ofSeconds(30));
        kafkaConnectClient.resetOffsets(testConnectorName);
        kafkaConnectClient.resumeConnector(testConnectorName);
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));
    }
}
