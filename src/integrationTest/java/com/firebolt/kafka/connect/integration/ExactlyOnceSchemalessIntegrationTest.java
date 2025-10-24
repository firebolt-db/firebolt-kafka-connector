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
public class ExactlyOnceSchemalessIntegrationTest extends SchemalessBaseIntegrationTest {

    private static final String TABLE_NAME = generateTableName("exactly_once_schemaless_it_table");
    private static final String TOPIC_NAME = generateTopicName("exactly-once-schemaless-it-topic");
    private static final Integer PARTITION_0 = 0;
    private static final Integer PARTITION_1 = 1;

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("exactly-once-it");

        // Create table, topic, and register connector with exactlyOnce=true
        Map<String, String> overrides = new HashMap<>();
        overrides.put("exactlyOnce", "true");
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, () -> String.format("CREATE FACT TABLE \"%s\" (id INT, value TEXT)", "%s"), overrides, TopicOptions.builder().partitions(2).build());
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(1));
        }
        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);
        removeMetadataTable();
        super.tearDown();
    }

    @Test
    void updatesMetadataAndIngestsRows() throws Exception {
        producer = initializeSchemalessJsonProducer();
        Map<Integer, Long> initialMeta = readMetadataOffsets(TOPIC_NAME);
        assertTrue(initialMeta.isEmpty(), "Metadata offsets should be empty initially");

        // send 2 records, one per partition (0 and 1)
        sendToPartition(PARTITION_0, new SimpleRecord(1, "a"));
        sendToPartition(PARTITION_1, new SimpleRecord(2, "b"));

        waitForDataInFirebolt(TABLE_NAME, 2);

        // verify metadata offsets: at least 0 for both partitions
        Map<Integer, Long> meta = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, meta.size());
        assertEquals(0L, meta.get(0));
        assertEquals(0L, meta.get(1));
    }

    @Test
    void resettingOffsetsPreventsDuplicateProcessing() throws Exception {
        producer = initializeSchemalessJsonProducer();
        // 2 records on partition 0
        sendToPartition(PARTITION_0, new SimpleRecord(10, "x"));
        sendToPartition(PARTITION_0, new SimpleRecord(11, "y"));

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
        sendToPartition(PARTITION_0, new SimpleRecord(12, "z"));
        waitForDataInFirebolt(TABLE_NAME, 3);

        Map<Integer, Long> metadataOffsets3 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets3.size());
        assertEquals(1L, metadataOffsets3.get(0));
        assertEquals(-1L, metadataOffsets3.get(1));
    }

    @Test
    void perPartitionOffsetsOnlyNewProcessed() throws Exception {
        producer = initializeSchemalessJsonProducer();
        sendToPartition(PARTITION_0, new SimpleRecord(100, "m"));
        sendToPartition(PARTITION_1, new SimpleRecord(200, "n"));
        waitForDataInFirebolt(TABLE_NAME, 2);

        Map<Integer, Long> metadataOffsets1 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets1.size());
        assertEquals(0L, metadataOffsets1.get(0));
        assertEquals(0L, metadataOffsets1.get(1));

        // send new data only to partition 1
        sendToPartition(PARTITION_1, new SimpleRecord(201, "n2"));
        waitForDataInFirebolt(TABLE_NAME, 3);

        Map<Integer, Long> metadataOffsets2 = readMetadataOffsets(TOPIC_NAME);
        assertEquals(2, metadataOffsets2.size());
        assertEquals(0L, metadataOffsets2.get(0));
        assertEquals(1L, metadataOffsets2.get(1));
    }

    @Test
    void multipleRestartsAndOffsetResetsPreservesExactlyOnceSemantics() throws Exception {
        producer = initializeSchemalessJsonProducer();
        
        // Initial data
        sendToPartition(PARTITION_0, new SimpleRecord(300, "initial"));
        waitForDataInFirebolt(TABLE_NAME, 1);
        assertEquals(0L, readMetadataOffsets(TOPIC_NAME).get(0));

        // First offset reset
        stopConnectorResetOffsetsAndRestartConnector();

        // Send new data
        sendToPartition(PARTITION_0, new SimpleRecord(301, "new"));
        waitForDataInFirebolt(TABLE_NAME, 2);
        assertEquals(1L, readMetadataOffsets(TOPIC_NAME).get(0));

        // Second offset reset
        stopConnectorResetOffsetsAndRestartConnector();

        // Send truly new data
        sendToPartition(PARTITION_0, new SimpleRecord(302, "final"));
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

    private void sendToPartition(int partition, SimpleRecord record) throws Exception {
        String key = "exactly-once-schemaless-key-" + record.getId();
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(TOPIC_NAME, partition, key, mapper.writeValueAsString(record));
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send record to partition {}: {}", partition, exception.getMessage());
            } else {
                log.info("Sent record to partition {}: offset {}", partition, metadata.offset());
            }
        });
        producer.flush();
    }
}
