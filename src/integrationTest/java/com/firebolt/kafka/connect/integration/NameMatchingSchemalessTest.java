package com.firebolt.kafka.connect.integration;

import static com.firebolt.kafka.connect.integration.NameMatchingSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.firebolt.kafka.connect.utils.TestTag;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Record-field ↔ column name matching on the schemaless JSON path ({@code read_json}): extra table
 * columns (with and without DEFAULT, nullable and NOT NULL), mixed record shapes within one batch,
 * explicit nulls vs absent keys, and record fields with no matching column (extra, case mismatch).
 *
 * <p>Records are produced <b>before</b> the connector starts so they arrive in a single poll batch —
 * the case where {@code read_json}'s batch-wide schema inference would otherwise turn an absent key
 * into an explicit NULL.
 */
@Slf4j
@Tag(TestTag.CONNECTOR)
public class NameMatchingSchemalessTest extends SchemalessBaseIntegrationTest {

    private final String TABLE_NAME = generateTableName("name_matching_json");
    private final String TOPIC_NAME = generateTopicName("name-matching-json");
    private String dlqTopicName;

    private Producer<String, String> producer;
    private KafkaConsumer<byte[], byte[]> dlqConsumer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("name-matching-json");
        dlqTopicName = TOPIC_NAME + "-dlq";
        try {
            cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);
            createTable(NameMatchingSupport.TABLE_SCHEMA, TABLE_NAME);
            createKafkaTopic(TOPIC_NAME);
        } catch (Exception e) {
            throw new RuntimeException("Test resources setup failed", e);
        }
        producer = initializeSchemalessJsonProducer();
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (dlqConsumer != null) {
            dlqConsumer.close();
        }
        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);
        safelyDeleteKafkaTopic(dlqTopicName);
        super.tearDown();
    }

    @Test
    void mixedShapesInOneBatchEachTakeTheirOwnDefaults() throws Exception {
        publish(
                "{\"id\":1}",                                                            // only the NOT NULL column
                "{\"id\":2,\"name\":\"b\",\"opt_def\":1}",                               // overrides one default
                "{\"req_def\":\"x\",\"id\":3}",                                          // overrides the NOT NULL default, reordered
                "{\"id\":4,\"opt_def\":null}",                                           // explicit null beats the default
                "{\"id\":5,\"name\":\"e\",\"opt\":\"o\",\"opt_def\":9,\"req_def\":\"y\"}"); // every column but ts_def

        // errors.tolerance=none: a single batch-level failure (e.g. NULL into req_def) would fail the task.
        registerSchemalessJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME,
                Map.of("errors.tolerance", "none"));
        waitForDataInFirebolt(TABLE_NAME, 5, Duration.ofSeconds(60));

        Map<Integer, List<Object>> rows = NameMatchingSupport.readRows(fireboltDefaultDbClient, TABLE_NAME);
        assertEquals(5, rows.size());
        assertEquals(row(null, null, 7, "rd"), rows.get(1));
        assertEquals(row("b", null, 1, "rd"), rows.get(2));
        assertEquals(row(null, null, 7, "x"), rows.get(3));
        assertEquals(row(null, null, null, "rd"), rows.get(4));
        assertEquals(row("e", "o", 9, "y"), rows.get(5));
    }

    @Test
    void unmatchedFieldsGoToDlqAndMatchingRecordsLand() throws Exception {
        publish(
                "{\"id\":10,\"name\":\"ok\"}",
                "{\"id\":11,\"extra\":\"x\"}",        // extra source field: no such column
                "{\"id\":12,\"Name\":\"case\"}",      // case mismatch: matching is exact
                "{\"name\":\"no id\"}",               // omits a NOT NULL column that has no default
                "{\"id\":13,\"req_def\":null}",       // explicit NULL into a NOT NULL column
                "{\"id\":14,\"opt_def\":2}");

        registerSchemalessJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME, Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", dlqTopicName,
                "errors.deadletterqueue.topic.replication.factor", "1"));
        waitForDataInFirebolt(TABLE_NAME, 2, Duration.ofSeconds(60));

        dlqConsumer = NameMatchingSupport.dlqConsumer(KAFKA_BOOTSTRAP_SERVERS);
        dlqConsumer.subscribe(Collections.singletonList(dlqTopicName));
        assertEquals(4, NameMatchingSupport.drainDlq(dlqConsumer, 4, Duration.ofSeconds(60)));

        Map<Integer, List<Object>> rows = NameMatchingSupport.readRows(fireboltDefaultDbClient, TABLE_NAME);
        assertEquals(2, rows.size());
        assertEquals(row("ok", null, 7, "rd"), rows.get(10));
        assertEquals(row(null, null, 2, "rd"), rows.get(14));
    }

    @Test
    void extraSourceFieldFailsTaskWithoutErrorTolerance() throws Exception {
        publish("{\"id\":20,\"name\":\"ok\",\"extra\":\"x\"}");

        registerSchemalessJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME,
                Map.of("errors.tolerance", "none"));

        NameMatchingSupport.awaitTaskFailed(httpClient, objectMapper, KAFKA_CONNECT_HOST, testConnectorName);
        assertEquals(0, fireboltDefaultDbClient.countRows(TABLE_NAME), "the table is the contract: nothing is dropped silently");
    }

    private void publish(String... values) throws Exception {
        for (int i = 0; i < values.length; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "k" + i, values[i])).get();
        }
        producer.flush();
    }
}
