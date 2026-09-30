package com.firebolt.kafka.connect.integration.avro;

import static com.firebolt.kafka.connect.integration.NameMatchingSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.firebolt.kafka.connect.integration.NameMatchingSupport;
import com.firebolt.kafka.connect.utils.TestTag;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Record-field ↔ column name matching on the Avro path ({@code read_avro}): an Avro schema covering
 * a reordered subset of the table's columns (the rest take their defaults), and an Avro schema with a
 * field that is not a column (every record goes to the DLQ; nothing lands).
 */
@Slf4j
@Tag(TestTag.CONNECTOR)
public class NameMatchingAvroTest extends AvroBaseIntegrationTest {

    private final String TABLE_NAME = generateTableName("name_matching_avro");
    private final String TOPIC_NAME = generateTopicName("name-matching-avro");
    private final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";
    private String dlqTopicName;

    private KafkaConsumer<byte[], byte[]> dlqConsumer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("name-matching-avro");
        dlqTopicName = TOPIC_NAME + "-dlq";
    }

    @AfterEach
    protected void tearDown() {
        if (dlqConsumer != null) {
            dlqConsumer.close();
        }
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        safelyDeleteKafkaTopic(dlqTopicName);
        super.tearDown();
    }

    @Test
    void reorderedSubsetOfColumnsTakesDefaultsForTheRest() throws Exception {
        String avroSchema = "{\"type\":\"record\",\"name\":\"Subset\",\"fields\":["
                + "{\"name\":\"req_def\",\"type\":\"string\"},"
                + "{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, NameMatchingSupport.TABLE_SCHEMA, () -> avroSchema,
                Map.of("errors.tolerance", "none"));

        Schema schema = new Schema.Parser().parse(avroSchema);
        publish(record(schema, 1, "a", "x"), record(schema, 2, null, "y"));
        waitForDataInFirebolt(TABLE_NAME, 2, Duration.ofSeconds(60));

        Map<Integer, List<Object>> rows = NameMatchingSupport.readRows(fireboltDefaultDbClient, TABLE_NAME);
        // opt (nullable, no default) -> NULL; opt_def -> DEFAULT 7; ts_def -> CURRENT_TIMESTAMP
        assertEquals(row("a", null, 7, "x"), rows.get(1));
        assertEquals(row(null, null, 7, "y"), rows.get(2)); // name: explicit Avro null
    }

    @Test
    void fieldWithNoMatchingColumnGoesToDlq() throws Exception {
        String avroSchema = "{\"type\":\"record\",\"name\":\"Extra\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
                + "{\"name\":\"extra\",\"type\":\"string\"}]}";
        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, NameMatchingSupport.TABLE_SCHEMA, () -> avroSchema,
                Map.of("errors.tolerance", "all",
                        "errors.deadletterqueue.topic.name", dlqTopicName,
                        "errors.deadletterqueue.topic.replication.factor", "1"));

        Schema schema = new Schema.Parser().parse(avroSchema);
        GenericData.Record first = new GenericData.Record(schema);
        first.put("id", 1);
        first.put("extra", "x");
        GenericData.Record second = new GenericData.Record(schema);
        second.put("id", 2);
        second.put("extra", "y");
        publish(first, second);

        dlqConsumer = NameMatchingSupport.dlqConsumer(KAFKA_BOOTSTRAP_SERVERS);
        dlqConsumer.subscribe(Collections.singletonList(dlqTopicName));
        assertEquals(2, NameMatchingSupport.drainDlq(dlqConsumer, 2, Duration.ofSeconds(60)));
        assertEquals(0, fireboltDefaultDbClient.countRows(TABLE_NAME));
    }

    private GenericData.Record record(Schema schema, int id, String name, String reqDef) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("name", name);
        record.put("req_def", reqDef);
        return record;
    }

    private void publish(GenericData.Record... records) throws Exception {
        try (Producer<String, Object> producer = initializeAvroProducer()) {
            for (int i = 0; i < records.length; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "k" + i, records[i])).get();
            }
            producer.flush();
        }
    }
}
