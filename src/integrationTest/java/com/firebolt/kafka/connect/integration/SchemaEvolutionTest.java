package com.firebolt.kafka.connect.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
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
 * Schema evolution on the schemaless JSON path ({@code read_json}), with no connector restart: the
 * table evolves while the connector runs, and one batch mixes records from before and after the change
 * (the connector is paused while they are produced, so they arrive in a single poll). Covers
 * {@code ADD COLUMN} with a nullable {@code DEFAULT} and a {@code NOT NULL DEFAULT}, {@code DROP COLUMN},
 * a producer that stops sending a field, and a straggler that still sends the dropped column.
 */
@Slf4j
@Tag(TestTag.CONNECTOR)
public class SchemaEvolutionTest extends SchemalessBaseIntegrationTest {

    private final String TABLE_NAME = generateTableName("schema_evolution_table");
    private final String TOPIC_NAME = generateTopicName("schema-evolution-topic");
    private final String DLQ_TOPIC = TOPIC_NAME + "-dlq";

    private Producer<String, String> producer;
    private KafkaConsumer<byte[], byte[]> dlqConsumer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("schema-evolution-test");
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
        safelyDeleteKafkaTopic(DLQ_TOPIC);
        super.tearDown();
    }

    @Test
    void evolvesTableAndProducersMidStreamWithoutRestart() throws Exception {
        Supplier<String> tableSchema = () -> "CREATE TABLE \"%s\" ("
                + "\"id\" INTEGER NOT NULL, \"name\" TEXT NULL, \"legacy\" TEXT NULL)";
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, tableSchema, Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", DLQ_TOPIC,
                "errors.deadletterqueue.topic.replication.factor", "1"));
        producer = initializeSchemalessJsonProducer();

        publish("{\"id\":1,\"name\":\"a\",\"legacy\":\"l\"}");
        waitForDataInFirebolt(TABLE_NAME, 1);

        NameMatchingSupport.pause(httpClient, objectMapper, KAFKA_CONNECT_HOST, testConnectorName);
        fireboltDefaultDbClient.executeUpdate(String.format(
                "ALTER TABLE \"%s\" ADD COLUMN \"score\" INTEGER NULL DEFAULT 7", TABLE_NAME));
        fireboltDefaultDbClient.executeUpdate(String.format(
                "ALTER TABLE \"%s\" ADD COLUMN \"tier\" TEXT NOT NULL DEFAULT 'free'", TABLE_NAME));
        fireboltDefaultDbClient.executeUpdate(String.format(
                "ALTER TABLE \"%s\" DROP COLUMN \"legacy\"", TABLE_NAME));
        // One batch straddling the change: old-shaped, new-shaped, and trimmed records, plus a straggler.
        publish(
                "{\"id\":2,\"name\":\"b\"}",                               // old shape, legacy no longer sent
                "{\"id\":3,\"name\":\"c\",\"score\":42,\"tier\":\"pro\"}", // new shape
                "{\"id\":4,\"score\":5}",                                  // producer dropped "name"
                "{\"id\":5,\"name\":\"e\",\"legacy\":\"l\"}");             // straggler: dropped column
        NameMatchingSupport.resume(httpClient, KAFKA_CONNECT_HOST, testConnectorName);
        waitForDataInFirebolt(TABLE_NAME, 4, Duration.ofSeconds(60));

        Map<Integer, List<Object>> rows = new HashMap<>();
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(String.format(
                "SELECT \"id\", \"name\", \"score\", \"tier\" FROM \"%s\"", TABLE_NAME))) {
            while (rs.next()) {
                Object score = rs.getObject("score");
                rows.put(rs.getInt("id"), Arrays.asList(rs.getString("name"),
                        score == null ? null : ((Number) score).intValue(), rs.getString("tier")));
            }
        }
        assertEquals(4, rows.size(), "the straggler must not land: " + rows);
        assertEquals(Arrays.asList("a", 7, "free"), rows.get(1)); // ingested before ADD COLUMN: backfilled defaults
        assertEquals(Arrays.asList("b", 7, "free"), rows.get(2)); // old shape after the change: defaults
        assertEquals(Arrays.asList("c", 42, "pro"), rows.get(3));
        assertEquals(Arrays.asList(null, 5, "free"), rows.get(4));

        dlqConsumer = NameMatchingSupport.dlqConsumer(KAFKA_BOOTSTRAP_SERVERS);
        dlqConsumer.subscribe(Collections.singletonList(DLQ_TOPIC));
        assertEquals(1, NameMatchingSupport.drainDlq(dlqConsumer, 1, Duration.ofSeconds(60)),
                "the record still carrying the dropped column goes to the DLQ");
        assertTrue(rows.values().stream().noneMatch(r -> "e".equals(r.get(0))));
    }

    private void publish(String... values) throws Exception {
        for (int i = 0; i < values.length; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "k" + i, values[i])).get();
        }
        producer.flush();
    }
}
