package com.firebolt.kafka.connect.integration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Integration tests for schema evolution: the connector should automatically pick up
 * columns that are added to a Firebolt table after the connector has started,
 * without any DDL being issued by the connector itself.
 *
 * Schema refresh is periodic (configurable interval). These tests use a short interval
 * (5 s) so they run in a reasonable time.
 *
 * Each test gets unique topic and table names to avoid Kafka's asynchronous topic-deletion
 * state bleeding into the next test when the same name is reused.
 */
@Slf4j
public class SchemaEvolutionIntegrationTest extends SchemalessBaseIntegrationTest {

    /** Short interval so tests don't have to wait 5 minutes. */
    private static final long REFRESH_INTERVAL_MS = 5_000L;

    /** How long to sleep after ALTER TABLE to guarantee the refresh interval has elapsed. */
    private static final long SLEEP_AFTER_ALTER_MS = REFRESH_INTERVAL_MS + 2_000L;

    // Per-test unique names — set in @BeforeEach so every test gets a fresh topic/table.
    private String tableName;
    private String topicName;

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("schema-evolution-test");
        String uid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        tableName = generateTableName("schema_ev_" + uid);
        topicName = generateTopicName("schema-ev-" + uid);
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }
        cleanupSchemalessTestResources(tableName, topicName);
        super.tearDown();
    }

    /**
     * A nullable column is added to Firebolt after the connector has started.
     * Kafka records that include the new field should have it populated in Firebolt
     * once the connector's periodic schema refresh has run.
     *
     * Records sent BEFORE the schema change should land correctly (new column is NULL,
     * since it didn't exist yet when those records were processed).
     */
    @Test
    void newNullableColumnIsPickedUpAfterRefresh() throws Exception {
        setupSchemalessTestResources(topicName, tableName, baseTableSchema(),
                schemaEvolutionConnectorOverrides());

        producer = initializeSchemalessJsonProducer();

        // Send records before the schema change — these should land without "extra"
        publishMessages(List.of(
                row(1, "alice"),
                row(2, "bob"),
                row(3, "carol")
        ));
        waitForDataInFirebolt(tableName, 3);

        // DBA adds a nullable column directly in Firebolt
        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"extra\" TEXT NULL");

        // Wait long enough that the connector's next put() will trigger a schema refresh
        Thread.sleep(SLEEP_AFTER_ALTER_MS);

        // Now send records that include the new field
        publishMessages(List.of(
                row(4, "dave",  "extra", "value-4"),
                row(5, "eve",   "extra", "value-5"),
                row(6, "frank", "extra", "value-6")
        ));
        waitForDataInFirebolt(tableName, 6);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {

            // Rows 1-3: landed before schema change — "extra" was not in the schema yet
            assertNextRow(rs, 1, "alice", null);
            assertNextRow(rs, 2, "bob",   null);
            assertNextRow(rs, 3, "carol", null);

            // Rows 4-6: landed after refresh — "extra" should be populated
            assertNextRow(rs, 4, "dave",  "value-4");
            assertNextRow(rs, 5, "eve",   "value-5");
            assertNextRow(rs, 6, "frank", "value-6");
        }
    }

    /**
     * A nullable column is added to Firebolt, but Kafka records never include that field.
     * The connector should pick up the new column in its schema cache (no errors),
     * and all rows should have NULL for the new column.
     */
    @Test
    void newNullableColumnNotInKafkaRecordsStaysNull() throws Exception {
        setupSchemalessTestResources(topicName, tableName, baseTableSchema(),
                schemaEvolutionConnectorOverrides());

        producer = initializeSchemalessJsonProducer();

        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"extra\" TEXT NULL");

        Thread.sleep(SLEEP_AFTER_ALTER_MS);

        // Records intentionally do not include "extra"
        publishMessages(List.of(
                row(1, "alice"),
                row(2, "bob")
        ));
        waitForDataInFirebolt(tableName, 2);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            assertNextRow(rs, 1, "alice", null);
            assertNextRow(rs, 2, "bob",   null);
        }
    }

    /**
     * A NOT NULL column with a DEFAULT value is added to Firebolt.
     * Kafka records that include the new field should have it populated from the record value.
     * Kafka records that do NOT include the new field should get the DEFAULT from Firebolt.
     */
    @Test
    void newNotNullColumnWithDefaultIsPickedUpAfterRefresh() throws Exception {
        setupSchemalessTestResources(topicName, tableName, baseTableSchema(),
                schemaEvolutionConnectorOverrides());

        producer = initializeSchemalessJsonProducer();

        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName
                + "\" ADD COLUMN \"extra\" TEXT NOT NULL DEFAULT 'default_val'");

        Thread.sleep(SLEEP_AFTER_ALTER_MS);

        // Record 1 has the field — should use the record value
        // Record 2 does not have the field — should get the Firebolt DEFAULT
        publishMessages(List.of(
                row(1, "alice", "extra", "custom_val"),
                row(2, "bob")                     // no "extra" key → column gets DEFAULT
        ));
        waitForDataInFirebolt(tableName, 2);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            assertNextRow(rs, 1, "alice", "custom_val");
            assertNextRow(rs, 2, "bob",   "default_val");
        }
    }

    /**
     * Auto schema pickup is disabled (default).
     * A column is added to Firebolt, but the connector never refreshes its schema.
     * Kafka record fields for the new column should be silently dropped and the
     * column should remain NULL for all rows.
     */
    @Test
    void autoSchemaPickupDisabled_newColumnIsIgnored() throws Exception {
        // auto.schema.pickup defaults to false — no override needed
        setupSchemalessTestResources(topicName, tableName, baseTableSchema(),
                Map.of());

        producer = initializeSchemalessJsonProducer();

        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"extra\" TEXT NULL");

        // Even if we wait, the connector should never pick up the new column
        Thread.sleep(SLEEP_AFTER_ALTER_MS);

        publishMessages(List.of(
                row(1, "alice", "extra", "value-1"),
                row(2, "bob",   "extra", "value-2")
        ));
        waitForDataInFirebolt(tableName, 2);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            // "extra" field from Kafka records was silently dropped — column stays NULL
            assertNextRow(rs, 1, "alice", null);
            assertNextRow(rs, 2, "bob",   null);
        }
    }

    /**
     * A nullable column is dropped from Firebolt after the connector has started.
     * <p>
     * Scenario A — records arrive <em>after</em> the refresh interval: the connector picks up
     * the schema change on its periodic refresh and subsequent records land cleanly with the
     * dropped field silently ignored.
     * <p>
     * Scenario B — records arrive <em>before</em> the refresh interval fires: the connector
     * detects the insert failure caused by the stale schema, immediately forces a schema refresh,
     * and retries the batch successfully within the same {@code put()} call.  No records are lost
     * and no task restart is required.
     */
    @Test
    void droppedColumnIsRemovedFromCacheAndIgnoredOnInsert() throws Exception {
        // Start with a table that has an "extra" column
        Supplier<String> schemaWithExtra = () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"name\" TEXT NULL, " +
                "\"extra\" TEXT NULL" +
                ")";
        setupSchemalessTestResources(topicName, tableName, schemaWithExtra,
                schemaEvolutionConnectorOverrides());

        producer = initializeSchemalessJsonProducer();

        // Send a row while "extra" still exists — it should be stored
        publishMessages(List.of(row(1, "alice", "extra", "value-1")));
        waitForDataInFirebolt(tableName, 1);

        // DBA drops the column directly in Firebolt — connector has NOT refreshed yet
        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName + "\" DROP COLUMN \"extra\"");

        // Send records immediately (before the refresh interval elapses).
        // The connector's first insert attempt will fail because the cached schema still
        // references "extra".  The connector must self-heal: force a schema refresh and
        // retry, completing without a task failure.
        publishMessages(List.of(
                row(2, "bob",   "extra", "value-2"),
                row(3, "carol", "extra", "value-3")
        ));
        waitForDataInFirebolt(tableName, 3);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            assertEquals(true, rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));

            assertEquals(true, rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("bob", rs.getString("name"));

            assertEquals(true, rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("carol", rs.getString("name"));
        }
    }

    /**
     * Two nullable columns are added to Firebolt simultaneously.
     * The connector's schema refresh must pick up both new columns in one pass
     * and start populating them correctly from subsequent Kafka records.
     */
    @Test
    void multipleColumnsAddedAtOnceAreAllPickedUp() throws Exception {
        setupSchemalessTestResources(topicName, tableName, baseTableSchema(),
                schemaEvolutionConnectorOverrides());

        producer = initializeSchemalessJsonProducer();

        // Add two columns at once
        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"extra1\" TEXT NULL");
        fireboltDefaultDbClient.executeUpdate(
                "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"extra2\" INTEGER NULL");

        Thread.sleep(SLEEP_AFTER_ALTER_MS);

        // Send records with both new fields
        publishMessages(List.of(
                row(1, "alice", "extra1", "val-a", "extra2", 10),
                row(2, "bob",   "extra1", "val-b", "extra2", 20)
        ));
        waitForDataInFirebolt(tableName, 2);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra1\", \"extra2\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            assertNextRichRow(rs, 1, "alice", "val-a", 10);
            assertNextRichRow(rs, 2, "bob",   "val-b", 20);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Supplier<String> baseTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"name\" TEXT NULL" +
                ")";
    }

    private Map<String, String> schemaEvolutionConnectorOverrides() {
        return Map.of(
                "schema.refresh.enabled", "true",
                "schema.refresh.interval.ms", String.valueOf(REFRESH_INTERVAL_MS)
        );
    }

    /**
     * Produces a JSON object with id, name, and any additional columns supplied as
     * alternating key/value varargs (e.g. {@code row(1, "alice", "extra", "val", "extra2", 42)}).
     * Null values are omitted from the JSON so the corresponding Firebolt column receives its
     * DEFAULT (or NULL for nullable columns).
     */
    private String row(int id, String name, Object... keyValuePairs) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"id\":").append(id).append(",");
        sb.append("\"name\":").append(name == null ? "null" : "\"" + name + "\"");
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            String key = (String) keyValuePairs[i];
            Object val = keyValuePairs[i + 1];
            if (val != null) {
                sb.append(",\"").append(key).append("\":");
                if (val instanceof String) {
                    sb.append("\"").append(val).append("\"");
                } else {
                    sb.append(val);
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private void publishMessages(List<String> jsonPayloads) throws Exception {
        for (String payload : jsonPayloads) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, payload);
            producer.send(record).get();
        }
        producer.flush();
    }

    private void assertNextRow(ResultSet rs, int expectedId, String expectedName, String expectedExtra)
            throws SQLException {
        assertEquals(true, rs.next(), "Expected another row in result set");
        assertEquals(expectedId, rs.getInt("id"), "id mismatch");
        assertEquals(expectedName, rs.getString("name"), "name mismatch");
        if (expectedExtra == null) {
            assertNull(rs.getString("extra"), "expected extra to be NULL");
        } else {
            assertEquals(expectedExtra, rs.getString("extra"), "extra mismatch");
        }
    }

    private void assertNextRichRow(ResultSet rs, int expectedId, String expectedName,
            String expectedExtra1, int expectedExtra2) throws SQLException {
        assertEquals(true, rs.next(), "Expected another row in result set");
        assertEquals(expectedId, rs.getInt("id"), "id mismatch");
        assertEquals(expectedName, rs.getString("name"), "name mismatch");
        assertEquals(expectedExtra1, rs.getString("extra1"), "extra1 mismatch");
        assertEquals(expectedExtra2, rs.getInt("extra2"), "extra2 mismatch");
    }
}
