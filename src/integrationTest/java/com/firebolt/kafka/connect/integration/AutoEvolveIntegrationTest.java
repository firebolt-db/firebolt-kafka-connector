package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.AutoEvolveRecord;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Integration tests for {@code auto.evolve}: the connector issues ALTER TABLE ADD COLUMN
 * in Firebolt when the Kafka Connect schema (Avro / JSON Schema / Protobuf) contains a field
 * that is absent from the target table.
 *
 * <p>The Firebolt table is created with only {@code id} and {@code name}; the schema registered
 * in Schema Registry also declares {@code extra} (and optionally more fields). When
 * {@code auto.evolve=true} the connector detects the mismatch on the first batch and issues
 * DDL before inserting.
 *
 * <p>Schemaless JSON records are out of scope: without a Connect schema there is no type
 * information for inference, so evolution is never triggered ({@code record.valueSchema() == null}).
 */
@Slf4j
public class AutoEvolveIntegrationTest extends SchemaBaseIntegrationTest {

    private String tableName;
    private String topicName;
    private String schemaSubject;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("auto-evolve-test");
        String uid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        tableName     = generateTableName("auto_evolve_" + uid);
        topicName     = generateTopicName("auto-evolve-" + uid);
        schemaSubject = topicName + "-value";
    }

    @AfterEach
    protected void tearDown() {
        cleanupTestResources(tableName, topicName, schemaSubject);
        super.tearDown();
    }

    /**
     * Happy path: the Kafka schema includes {@code extra} but the Firebolt table does not.
     * With {@code auto.evolve=true} the connector must ADD the column and populate it.
     */
    @Test
    void newColumnInSchemaIsAddedToFirebolt() throws Exception {
        setupTestResources(topicName, tableName, schemaSubject,
                baseTableSchema(), oneExtraColumnSchema(), Map.of("auto.evolve", "true"));

        Producer<String, AutoEvolveRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName, new AutoEvolveRecord(1, "alice", "val-1"))).get();
        producer.send(new ProducerRecord<>(topicName, new AutoEvolveRecord(2, "bob",   "val-2"))).get();
        producer.flush();
        producer.close();

        waitForDataInFirebolt(tableName, 2);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            assertNextRow(rs, 1, "alice", "val-1");
            assertNextRow(rs, 2, "bob",   "val-2");
        }
    }

    /**
     * With {@code auto.evolve=false} (the default): a field present in the Kafka schema but
     * absent from Firebolt must never trigger DDL — the column must not be created.
     */
    @Test
    void autoEvolveDisabled_newColumnInSchemaIsNotAddedToFirebolt() throws Exception {
        setupTestResources(topicName, tableName, schemaSubject,
                baseTableSchema(), oneExtraColumnSchema(), Collections.emptyMap());

        Producer<String, AutoEvolveRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName, new AutoEvolveRecord(1, "alice", "val-1"))).get();
        producer.flush();
        producer.close();

        waitForDataInFirebolt(tableName, 1);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT column_name FROM information_schema.columns"
                + " WHERE table_name = '" + tableName + "' AND column_name = 'extra'")) {
            assertFalse(rs.next(), "'extra' column must not exist when auto.evolve=false");
        }
    }

    /**
     * Two new columns ({@code extra1} TEXT and {@code extra2} INTEGER) absent from Firebolt
     * but present in the Kafka schema must both be added in a single batch.
     */
    @Test
    void multipleNewColumnsAreAllAdded() throws Exception {
        setupTestResources(topicName, tableName, schemaSubject,
                baseTableSchema(), twoExtraColumnsSchema(), Map.of("auto.evolve", "true"));

        Producer<String, TwoExtraRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName, new TwoExtraRecord(1, "alice", "a1", 10))).get();
        producer.send(new ProducerRecord<>(topicName, new TwoExtraRecord(2, "bob",   "b2", 20))).get();
        producer.flush();
        producer.close();

        waitForDataInFirebolt(tableName, 2);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra1\", \"extra2\" FROM \""
                        + tableName + "\" ORDER BY \"id\"")) {
            assertEquals(true, rs.next());
            assertEquals(1,       rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));
            assertEquals("a1",    rs.getString("extra1"));
            assertEquals(10,      rs.getInt("extra2"));

            assertEquals(true, rs.next());
            assertEquals(2,     rs.getInt("id"));
            assertEquals("bob", rs.getString("name"));
            assertEquals("b2",  rs.getString("extra1"));
            assertEquals(20,    rs.getInt("extra2"));
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Firebolt table DDL — intentionally omits the 'extra' column(s). */
    private Supplier<String> baseTableSchema() {
        return () -> "CREATE TABLE \"%s\" (\"id\" INTEGER NOT NULL, \"name\" TEXT NULL)";
    }

    /** JSON Schema with one extra TEXT field absent from the Firebolt table. */
    private Supplier<String> oneExtraColumnSchema() {
        return () -> "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"type\": \"object\",\n"
                + "  \"additionalProperties\": false,\n"
                + "  \"properties\": {\n"
                + "    \"id\":    {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\n"
                + "    \"name\":  {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]},\n"
                + "    \"extra\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]}\n"
                + "  }\n"
                + "}";
    }

    /** JSON Schema with two extra fields absent from the Firebolt table. */
    private Supplier<String> twoExtraColumnsSchema() {
        return () -> "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"type\": \"object\",\n"
                + "  \"additionalProperties\": false,\n"
                + "  \"properties\": {\n"
                + "    \"id\":     {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\n"
                + "    \"name\":   {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]},\n"
                + "    \"extra1\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]},\n"
                + "    \"extra2\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]}\n"
                + "  }\n"
                + "}";
    }

    private void assertNextRow(ResultSet rs, int expectedId, String expectedName, String expectedExtra)
            throws java.sql.SQLException {
        assertEquals(true, rs.next(), "Expected another row");
        assertEquals(expectedId,   rs.getInt("id"),         "id mismatch");
        assertEquals(expectedName, rs.getString("name"),    "name mismatch");
        assertEquals(expectedExtra, rs.getString("extra"),  "extra mismatch");
    }

    /** POJO for the two-extra-columns test. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TwoExtraRecord {
        private Integer id;
        private String name;
        private String extra1;
        private Integer extra2;
    }
}
