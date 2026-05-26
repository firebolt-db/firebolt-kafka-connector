package com.firebolt.kafka.connect.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@code auto.evolve}: the connector issues ALTER TABLE ADD COLUMN
 * in Firebolt when the Kafka Connect schema (Avro / JSON Schema / Protobuf) contains a field
 * that is absent from the target table.
 *
 * <p>The Firebolt table starts with only the columns defined at creation time; additional fields
 * declared in the Schema Registry schema trigger DDL when {@code auto.evolve=true}.
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

    // =========================================================================
    // Basic happy-path and disabled-flag tests
    // =========================================================================

    /**
     * Happy path: the Kafka schema includes {@code extra} but the Firebolt table does not.
     * With {@code auto.evolve=true} the connector must ADD the column and populate it.
     */
    @Test
    void newColumnInSchemaIsAddedToFirebolt() throws Exception {
        setupTestResources(topicName, tableName, schemaSubject,
                baseTableSchema(), oneExtraColumnSchema(), Map.of("auto.evolve", "true"));

        Producer<String, SimpleEvolvingRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName, new SimpleEvolvingRecord(1, "alice", "val-1"))).get();
        producer.send(new ProducerRecord<>(topicName, new SimpleEvolvingRecord(2, "bob",   "val-2"))).get();
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

        Producer<String, SimpleEvolvingRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName, new SimpleEvolvingRecord(1, "alice", "val-1"))).get();
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
     * Two new columns ({@code extra} TEXT and {@code extra2} INTEGER) absent from Firebolt
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
                "SELECT \"id\", \"name\", \"extra\", \"extra2\" FROM \""
                        + tableName + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1,       rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));
            assertEquals("a1",    rs.getString("extra"));
            assertEquals(10,      rs.getInt("extra2"));

            assertTrue(rs.next());
            assertEquals(2,     rs.getInt("id"));
            assertEquals("bob", rs.getString("name"));
            assertEquals("b2",  rs.getString("extra"));
            assertEquals(20,    rs.getInt("extra2"));
        }
    }

    // =========================================================================
    // Mid-stream schema change
    // =========================================================================

    /**
     * Records arrive in two waves:
     * <ol>
     *   <li>Batch 1 uses schema v1 (id, name) — no DDL needed.</li>
     *   <li>A new schema version (id, name, extra) is registered in Schema Registry.</li>
     *   <li>Batch 2 uses schema v2 — connector detects the new field and issues DDL.</li>
     * </ol>
     * Both batches must land correctly, with batch-1 rows having {@code extra = NULL}
     * and batch-2 rows having the value supplied by the producer.
     */
    @Test
    void midStreamSchemaChange_newColumnPopulatedAfterEvolution() throws Exception {
        // Table and connector start with schema v1 (no 'extra' column)
        setupTestResources(topicName, tableName, schemaSubject,
                baseTableSchema(), baseJsonSchema(), Map.of("auto.evolve", "true"));

        // Batch 1 — schema v1 records
        Producer<String, BaseRecord> v1Producer = initializeJsonProducer();
        v1Producer.send(new ProducerRecord<>(topicName, new BaseRecord(1, "alice"))).get();
        v1Producer.send(new ProducerRecord<>(topicName, new BaseRecord(2, "bob"))).get();
        v1Producer.flush();
        v1Producer.close();

        waitForDataInFirebolt(tableName, 2);

        // Register schema v2 in Schema Registry (adds 'extra' field)
        getSchemaRegistryClient().registerSchema(schemaSubject, oneExtraColumnSchema().get(), "JSON");

        // Batch 2 — schema v2 records (producer picks up latest version via use.latest.version=true)
        Producer<String, SimpleEvolvingRecord> v2Producer = initializeJsonProducer();
        v2Producer.send(new ProducerRecord<>(topicName, new SimpleEvolvingRecord(3, "carol", "v2-val"))).get();
        v2Producer.send(new ProducerRecord<>(topicName, new SimpleEvolvingRecord(4, "dave",  "v2-val2"))).get();
        v2Producer.flush();
        v2Producer.close();

        waitForDataInFirebolt(tableName, 4);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\" FROM \"" + tableName + "\" ORDER BY \"id\"")) {
            // Batch-1 rows: 'extra' column added by DDL but value is NULL for pre-evolution rows
            assertTrue(rs.next());
            assertEquals(1,       rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));
            rs.getString("extra"); // column must exist (no SQLException)

            assertTrue(rs.next());
            assertEquals(2,     rs.getInt("id"));
            assertEquals("bob", rs.getString("name"));
            rs.getString("extra");

            // Batch-2 rows: 'extra' column populated with the value from the record
            assertTrue(rs.next());
            assertEquals(3,         rs.getInt("id"));
            assertEquals("carol",   rs.getString("name"));
            assertEquals("v2-val",  rs.getString("extra"));

            assertTrue(rs.next());
            assertEquals(4,          rs.getInt("id"));
            assertEquals("dave",     rs.getString("name"));
            assertEquals("v2-val2",  rs.getString("extra"));
        }
    }

    // =========================================================================
    // Concurrent workers
    // =========================================================================

    /**
     * Two connector tasks (tasks.max=2) consuming from 2 partitions may simultaneously issue
     * {@code ALTER TABLE ADD COLUMN IF NOT EXISTS "extra"}.  The {@code IF NOT EXISTS} clause
     * and the DDL retry logic must ensure both tasks complete without error and all rows land.
     */
    @Test
    void concurrentWorkers_alterTableIsIdempotent() throws Exception {
        // Create topic with 2 partitions to allow 2 tasks to run in parallel
        createKafkaTopic(topicName, com.firebolt.kafka.connect.utils.TopicOptions.withPartitions(2));

        // Register schema in Schema Registry
        getSchemaRegistryClient().registerSchema(schemaSubject, oneExtraColumnSchema().get(), "JSON");

        // Create the table (base schema, no 'extra')
        createTable(baseTableSchema(), tableName);

        // Register connector with tasks.max=2 and auto.evolve=true
        registerJsonConnector(testConnectorName, topicName, topicName + ":" + tableName,
                Map.of("auto.evolve", "true", "tasks.max", "2"));

        // Produce to both partitions to ensure both tasks get records that trigger DDL
        Producer<String, SimpleEvolvingRecord> producer = initializeJsonProducer();
        for (int i = 1; i <= 10; i++) {
            int partition = i % 2;
            producer.send(new ProducerRecord<>(topicName, partition, null,
                    new SimpleEvolvingRecord(i, "user-" + i, "val-" + i))).get();
        }
        producer.flush();
        producer.close();

        waitForDataInFirebolt(tableName, 10);

        // All 10 rows must be present
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT COUNT(*) AS cnt FROM \"" + tableName + "\"")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("cnt"));
        }

        // The 'extra' column must exist
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT column_name FROM information_schema.columns"
                + " WHERE table_name = '" + tableName + "' AND column_name = 'extra'")) {
            assertTrue(rs.next(), "'extra' column must exist after concurrent auto.evolve");
        }
    }

    // =========================================================================
    // Full scalar type coverage
    // =========================================================================

    /**
     * Verifies that all scalar Firebolt types reachable via Kafka Connect's type system are
     * correctly inferred and written:
     * <ul>
     *   <li>BIGINT (INT64)</li>
     *   <li>REAL (FLOAT32)</li>
     *   <li>DOUBLE PRECISION (FLOAT64)</li>
     *   <li>BOOLEAN</li>
     *   <li>TEXT (STRING)</li>
     * </ul>
     * The table starts with only {@code id INTEGER}; all other columns are added by auto.evolve.
     */
    @Test
    void allScalarTypesAreAddedAndPopulatedCorrectly() throws Exception {
        setupTestResources(topicName, tableName, schemaSubject,
                idOnlyTableSchema(), scalarTypesSchema(), Map.of("auto.evolve", "true"));

        Producer<String, ScalarTypesRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName,
                new ScalarTypesRecord(1, 9_000_000_000L, 3.14f, 2.718281828, true, "hello"))).get();
        producer.flush();
        producer.close();

        waitForDataInFirebolt(tableName, 1);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"big_num\", \"real_num\", \"double_num\", \"flag\", \"label\""
                + " FROM \"" + tableName + "\"")) {
            assertTrue(rs.next());
            assertEquals(1,              rs.getInt("id"));
            assertEquals(9_000_000_000L, rs.getLong("big_num"));
            assertEquals(3.14f,          rs.getFloat("real_num"),   0.001f);
            assertEquals(2.718281828,    rs.getDouble("double_num"), 0.0000001);
            assertEquals(true,           rs.getBoolean("flag"));
            assertEquals("hello",        rs.getString("label"));
        }
    }

    // =========================================================================
    // ARRAY type coverage
    // =========================================================================

    /**
     * Verifies that ARRAY(TEXT NULL) and ARRAY(INTEGER NULL) columns are correctly added by
     * auto.evolve and that array values round-trip through Firebolt without loss.
     */
    @Test
    void arrayTypesAreAddedAndPopulatedCorrectly() throws Exception {
        setupTestResources(topicName, tableName, schemaSubject,
                idOnlyTableSchema(), arrayTypesSchema(), Map.of("auto.evolve", "true"));

        Producer<String, ArrayTypesRecord> producer = initializeJsonProducer();
        producer.send(new ProducerRecord<>(topicName,
                new ArrayTypesRecord(1, List.of("a", "b", "c"), List.of(10, 20, 30)))).get();
        producer.flush();
        producer.close();

        waitForDataInFirebolt(tableName, 1);

        // Verify both array columns exist
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT column_name FROM information_schema.columns"
                + " WHERE table_name = '" + tableName
                + "' AND column_name IN ('text_arr', 'int_arr') ORDER BY column_name")) {
            assertTrue(rs.next(), "First array column must exist");
            assertTrue(rs.next(), "Second array column must exist");
        }

        // Verify the row landed and array elements are accessible
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"text_arr\"[1] AS t1, \"int_arr\"[1] AS i1"
                + " FROM \"" + tableName + "\"")) {
            assertTrue(rs.next());
            assertEquals(1,   rs.getInt("id"));
            assertEquals("a", rs.getString("t1"));
            assertEquals(10,  rs.getInt("i1"));
        }
    }

    // =========================================================================
    // Multiple schema iterations (progressive evolution)
    // =========================================================================

    /**
     * Schema evolves in three steps: v1 → v2 (+extra) → v3 (+extra, +extra2).
     * Each wave of records must land with all previously-added columns populated.
     * Verifies that the connector handles repeated DDL across multiple batches
     * within the same connector lifetime.
     */
    @Test
    void multipleSchemaIterations_columnsAddedProgressively() throws Exception {
        // Start: table with only id + name; schema v1 registered
        setupTestResources(topicName, tableName, schemaSubject,
                baseTableSchema(), baseJsonSchema(), Map.of("auto.evolve", "true"));

        // --- Wave 1: baseline records, no new columns ---
        Producer<String, BaseRecord> wave1 = initializeJsonProducer();
        wave1.send(new ProducerRecord<>(topicName, new BaseRecord(1, "alice"))).get();
        wave1.flush();
        wave1.close();
        waitForDataInFirebolt(tableName, 1);

        // --- Register schema v2: adds 'extra' TEXT ---
        getSchemaRegistryClient().registerSchema(schemaSubject, oneExtraColumnSchema().get(), "JSON");

        Producer<String, SimpleEvolvingRecord> wave2 = initializeJsonProducer();
        wave2.send(new ProducerRecord<>(topicName, new SimpleEvolvingRecord(2, "bob", "extra-val"))).get();
        wave2.flush();
        wave2.close();
        waitForDataInFirebolt(tableName, 2);

        // --- Register schema v3: adds 'extra' + 'extra2' INTEGER ---
        getSchemaRegistryClient().registerSchema(schemaSubject, twoExtraColumnsSchema().get(), "JSON");

        Producer<String, TwoExtraRecord> wave3 = initializeJsonProducer();
        wave3.send(new ProducerRecord<>(topicName, new TwoExtraRecord(3, "carol", "e1-val", 99))).get();
        wave3.flush();
        wave3.close();
        waitForDataInFirebolt(tableName, 3);

        // Verify final state
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"name\", \"extra\", \"extra2\" FROM \"" + tableName
                + "\" ORDER BY \"id\"")) {

            // Wave-1 row: extra and extra2 are NULL (columns didn't exist when this row was inserted)
            assertTrue(rs.next());
            assertEquals(1,       rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));
            rs.getString("extra");   // column must exist (no SQLException)
            rs.getInt("extra2");     // column must exist (no SQLException)

            // Wave-2 row: extra is populated, extra2 is NULL (column added later)
            assertTrue(rs.next());
            assertEquals(2,            rs.getInt("id"));
            assertEquals("bob",        rs.getString("name"));
            assertEquals("extra-val",  rs.getString("extra"));
            rs.getInt("extra2");

            // Wave-3 row: both columns populated
            assertTrue(rs.next());
            assertEquals(3,         rs.getInt("id"));
            assertEquals("carol",   rs.getString("name"));
            assertEquals("e1-val",  rs.getString("extra"));
            assertEquals(99,        rs.getInt("extra2"));
        }
    }

    // =========================================================================
    // Schema helpers
    // =========================================================================

    /** Firebolt DDL — table with only id + name; extra columns intentionally absent. */
    private Supplier<String> baseTableSchema() {
        return () -> "CREATE TABLE \"%s\" (\"id\" INTEGER NOT NULL, \"name\" TEXT NULL)";
    }

    /** Firebolt DDL — table with only id; used by type-coverage tests. */
    private Supplier<String> idOnlyTableSchema() {
        return () -> "CREATE TABLE \"%s\" (\"id\" INTEGER NOT NULL)";
    }

    /** JSON Schema v1: id + name only. */
    private Supplier<String> baseJsonSchema() {
        return () -> "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"type\": \"object\",\n"
                + "  \"additionalProperties\": false,\n"
                + "  \"properties\": {\n"
                + "    \"id\":   {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\n"
                + "    \"name\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]}\n"
                + "  }\n"
                + "}";
    }

    /** JSON Schema with one extra TEXT field ({@code extra}) absent from the Firebolt table. */
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

    /** JSON Schema with two extra fields: {@code extra} TEXT and {@code extra2} INTEGER. */
    private Supplier<String> twoExtraColumnsSchema() {
        return () -> "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"type\": \"object\",\n"
                + "  \"additionalProperties\": false,\n"
                + "  \"properties\": {\n"
                + "    \"id\":     {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\n"
                + "    \"name\":   {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]},\n"
                + "    \"extra\":  {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]},\n"
                + "    \"extra2\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]}\n"
                + "  }\n"
                + "}";
    }

    /** JSON Schema for scalar type coverage (BIGINT, REAL, DOUBLE PRECISION, BOOLEAN, TEXT). */
    private Supplier<String> scalarTypesSchema() {
        return () -> "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"type\": \"object\",\n"
                + "  \"additionalProperties\": false,\n"
                + "  \"properties\": {\n"
                + "    \"id\":         {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\n"
                + "    \"big_num\":    {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int64\"}]},\n"
                + "    \"real_num\":   {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"number\",\"connect.type\":\"float32\"}]},\n"
                + "    \"double_num\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"number\",\"connect.type\":\"float64\"}]},\n"
                + "    \"flag\":       {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"boolean\"}]},\n"
                + "    \"label\":      {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"string\"}]}\n"
                + "  }\n"
                + "}";
    }

    /** JSON Schema for ARRAY type coverage: ARRAY(TEXT NULL) and ARRAY(INTEGER NULL). */
    private Supplier<String> arrayTypesSchema() {
        return () -> "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"type\": \"object\",\n"
                + "  \"additionalProperties\": false,\n"
                + "  \"properties\": {\n"
                + "    \"id\":       {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]},\n"
                + "    \"text_arr\": {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]}}]},\n"
                + "    \"int_arr\":  {\"oneOf\": [{\"type\":\"null\"},{\"type\":\"array\",\"items\":{\"oneOf\":[{\"type\":\"null\"},{\"type\":\"integer\",\"connect.type\":\"int32\"}]}}]}\n"
                + "  }\n"
                + "}";
    }

    // =========================================================================
    // Assertion helpers
    // =========================================================================

    private void assertNextRow(ResultSet rs, int expectedId, String expectedName, String expectedExtra)
            throws java.sql.SQLException {
        assertTrue(rs.next(), "Expected another row");
        assertEquals(expectedId,    rs.getInt("id"),         "id mismatch");
        assertEquals(expectedName,  rs.getString("name"),    "name mismatch");
        assertEquals(expectedExtra, rs.getString("extra"),   "extra mismatch");
    }

    // =========================================================================
    // Inner record POJOs
    // =========================================================================

    /** Used by the single-extra-column tests. */
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class SimpleEvolvingRecord {
        private Integer id;
        private String  name;
        private String  extra;
    }

    /** Used by the base-schema tests (id + name only). */
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class BaseRecord {
        private Integer id;
        private String  name;
    }

    /** Used by the two-extra-columns tests. */
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class TwoExtraRecord {
        private Integer id;
        private String  name;
        private String  extra;
        private Integer extra2;
    }

    /** Used by the scalar-types test. */
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class ScalarTypesRecord {
        private Integer id;
        @JsonProperty("big_num")    private Long    bigNum;
        @JsonProperty("real_num")   private Float   realNum;
        @JsonProperty("double_num") private Double  doubleNum;
        private Boolean flag;
        private String  label;
    }

    /** Used by the ARRAY types test. */
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class ArrayTypesRecord {
        private Integer       id;
        @JsonProperty("text_arr") private List<String>  textArr;
        @JsonProperty("int_arr")  private List<Integer> intArr;
    }
}
