package com.firebolt.kafka.connect.integration.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.firebolt.kafka.connect.integration.NameMatchingSupport;
import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Schema evolution on the Avro / Schema Registry path ({@code read_avro}), with no connector restart:
 * the table gains columns while producers roll out new schema versions, and one batch mixes records
 * written with v1, v2 and v3 (the connector is paused while they are produced). v2 adds an optional
 * field and widens {@code int -> long}; v3 drops a field. Each writer schema is its own upload, so a
 * column absent from a record's schema takes its {@code DEFAULT}.
 */
@Slf4j
@Tag(TestTag.CONNECTOR)
public class SchemaEvolutionAvroTest extends AvroBaseIntegrationTest {

    private static final String V1 = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"big\",\"type\":\"int\"}]}";
    private static final String V2 = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"big\",\"type\":\"long\"},"
            + "{\"name\":\"score\",\"type\":[\"null\",\"int\"],\"default\":null}]}";
    private static final String V3 = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"int\"},"
            + "{\"name\":\"big\",\"type\":\"long\"},"
            + "{\"name\":\"score\",\"type\":[\"null\",\"int\"],\"default\":null}]}";

    private final String TABLE_NAME = generateTableName("schema_evolution_avro");
    private final String TOPIC_NAME = generateTopicName("schema-evolution-avro");
    private final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, Object> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("schema-evolution-avro");
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @Test
    void evolvesTableAndWriterSchemasMidStreamWithoutRestart() throws Exception {
        Supplier<String> tableSchema = () -> "CREATE TABLE \"%s\" ("
                + "\"id\" INTEGER NOT NULL, \"name\" TEXT NULL, \"big\" BIGINT NULL)";
        // errors.tolerance=none: any record that doesn't land fails the task and the test.
        // use.latest.version=false (the converter default): each record keeps its own writer schema.
        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, tableSchema, () -> V1, Map.of(
                "errors.tolerance", "none",
                "value.converter.use.latest.version", "false"));
        producer = evolvingProducer();
        Schema v1 = new Schema.Parser().parse(V1);
        Schema v2 = new Schema.Parser().parse(V2);
        Schema v3 = new Schema.Parser().parse(V3);

        publish(record(v1, "id", 1, "name", "a", "big", 1));
        waitForDataInFirebolt(TABLE_NAME, 1);

        NameMatchingSupport.pause(httpClient, objectMapper, KAFKA_CONNECT_HOST, testConnectorName);
        fireboltDefaultDbClient.executeUpdate(String.format(
                "ALTER TABLE \"%s\" ADD COLUMN \"score\" INTEGER NULL DEFAULT 7", TABLE_NAME));
        fireboltDefaultDbClient.executeUpdate(String.format(
                "ALTER TABLE \"%s\" ADD COLUMN \"tier\" TEXT NOT NULL DEFAULT 'free'", TABLE_NAME));
        publish(
                record(v1, "id", 2, "name", "b", "big", 2),                             // old producer
                record(v2, "id", 3, "name", "c", "big", 1L << 40, "score", 42),         // int -> long, new field
                record(v2, "id", 4, "name", "d", "big", 4L, "score", null),             // explicit null
                record(v3, "id", 5, "big", 5L, "score", 9));                            // "name" dropped
        NameMatchingSupport.resume(httpClient, KAFKA_CONNECT_HOST, testConnectorName);
        waitForDataInFirebolt(TABLE_NAME, 5, Duration.ofSeconds(60));

        Map<Integer, List<Object>> rows = new HashMap<>();
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(String.format(
                "SELECT \"id\", \"name\", \"big\", \"score\", \"tier\" FROM \"%s\"", TABLE_NAME))) {
            while (rs.next()) {
                Object score = rs.getObject("score");
                rows.put(rs.getInt("id"), Arrays.asList(rs.getString("name"), rs.getLong("big"),
                        score == null ? null : ((Number) score).intValue(), rs.getString("tier")));
            }
        }
        assertEquals(Arrays.asList("a", 1L, 7, "free"), rows.get(1));        // backfilled by ADD COLUMN
        assertEquals(Arrays.asList("b", 2L, 7, "free"), rows.get(2));        // v1 lacks score/tier: defaults
        assertEquals(Arrays.asList("c", 1L << 40, 42, "free"), rows.get(3)); // widened value lands
        assertEquals(Arrays.asList("d", 4L, null, "free"), rows.get(4));     // field in schema, value null: NULL
        assertEquals(Arrays.asList(null, 5L, 9, "free"), rows.get(5));       // v3 lacks name: NULL
    }

    /** Registers each record's writer schema on first use, like a producer rolling out a new version. */
    private Producer<String, Object> evolvingProducer() {
        Properties props = createBasicProducerProperties(true);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", "true");
        props.put("use.latest.version", "false");
        return new KafkaProducer<>(props);
    }

    private static GenericData.Record record(Schema schema, Object... fieldsAndValues) {
        GenericData.Record record = new GenericData.Record(schema);
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            record.put((String) fieldsAndValues[i], fieldsAndValues[i + 1]);
        }
        return record;
    }

    private void publish(GenericData.Record... records) throws Exception {
        for (int i = 0; i < records.length; i++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "k" + i, records[i])).get();
        }
        producer.flush();
    }
}
