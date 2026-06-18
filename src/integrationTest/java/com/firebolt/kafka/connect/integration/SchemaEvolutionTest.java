package com.firebolt.kafka.connect.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Verifies Firebolt-side schema evolution is absorbed with no connector restart and no connector
 * awareness of the table schema: after {@code ALTER TABLE … ADD COLUMN}, records carrying the new
 * field land in the new column, while older-shaped records (without it) keep landing with the
 * column defaulted. Because the connector builds each INSERT from the record's own field names and
 * never caches the table schema, the ALTER takes effect on the very next batch.
 */
@Slf4j
@Tag(TestTag.CONNECTOR)
public class SchemaEvolutionTest extends SchemalessBaseIntegrationTest {

    private final String TABLE_NAME = generateTableName("schema_evolution_table");
    private final String TOPIC_NAME = generateTopicName("schema-evolution-topic");

    private Producer<String, String> producer;

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
        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);
        super.tearDown();
    }

    @Test
    void absorbsAddColumnMidStreamWithoutRestart() throws Exception {
        Supplier<String> tableSchema = () -> "CREATE TABLE \"%s\" ("
                + "\"id\" INTEGER NOT NULL, "
                + "\"name\" TEXT NULL)";
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, tableSchema);
        producer = initializeSchemalessJsonProducer();

        // Phase 1: ingest records shaped to the original (id, name) schema.
        publish("k1", "{\"id\":1,\"name\":\"alice\"}");
        publish("k2", "{\"id\":2,\"name\":\"bob\"}");
        waitForDataInFirebolt(TABLE_NAME, 2);

        // Evolve the table while the connector keeps running. It caches no schema, so no restart.
        fireboltDefaultDbClient.executeUpdate(
                String.format("ALTER TABLE \"%s\" ADD COLUMN \"score\" INTEGER NULL", TABLE_NAME));

        // Phase 2: one record carrying the NEW field, and one still in the old shape (no score).
        publish("k3", "{\"id\":3,\"name\":\"carol\",\"score\":42}");
        publish("k4", "{\"id\":4,\"name\":\"dave\"}");
        waitForDataInFirebolt(TABLE_NAME, 4);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                String.format("SELECT \"id\", \"name\", \"score\" FROM \"%s\" ORDER BY \"id\"", TABLE_NAME))) {
            assertRow(rs, 1, "alice", null); // ingested before the column existed -> NULL
            assertRow(rs, 2, "bob", null);
            assertRow(rs, 3, "carol", 42);   // carried the new field -> lands in the new column
            assertRow(rs, 4, "dave", null);  // old-shaped after the ALTER -> column defaulted
        }
    }

    private void publish(String key, String json) throws Exception {
        producer.send(new ProducerRecord<>(TOPIC_NAME, key, json)).get();
        producer.flush();
    }

    private void assertRow(ResultSet rs, int expectedId, String expectedName, Integer expectedScore) throws SQLException {
        assertTrue(rs.next(), "Expected a row for id=" + expectedId);
        assertEquals(expectedId, rs.getInt("id"));
        assertEquals(expectedName, rs.getString("name"));
        Object score = rs.getObject("score");
        if (expectedScore == null) {
            assertNull(score, "score should be NULL for id=" + expectedId);
        } else {
            assertEquals(expectedScore.intValue(), ((Number) score).intValue(), "score mismatch for id=" + expectedId);
        }
    }
}
