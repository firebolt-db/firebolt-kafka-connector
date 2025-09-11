package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleStringRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class DlqReporterIntegrationTest extends BaseIntegrationTest {

    private static final String TABLE_NAME = "dlq_it_table";
    private static final String TOPIC_NAME = "dlq-it-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private String dlqTopicName;
    private Producer<String, SimpleStringRecord> producer;
    private KafkaConsumer<String, String> dlqConsumer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("dlq-reporter-it");

        // Configure connector to use DLQ
        dlqTopicName = "dlq-" + RandomStringUtils.insecure().nextAlphabetic(5);
        Map<String, String> connectorPropertiesOverride = Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", dlqTopicName,
                "errors.deadletterqueue.context.headers.enable", "true"
        );
        // Ensure DLQ topic exists to avoid broker auto-creation dependencies
        createKafkaTopic(dlqTopicName);

        // Use local schema suppliers to guarantee table/schema alignment
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                tableSchemaSupplier(), jsonSchemaSupplier(), connectorPropertiesOverride);

        producer = initializeJsonProducer();
        dlqConsumer = createDlqConsumer();
        dlqConsumer.subscribe(Collections.singletonList(dlqTopicName));
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) producer.close();
        if (dlqConsumer != null) dlqConsumer.close();

        cleanupTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        safelyDeleteKafkaTopic(dlqTopicName);
        super.tearDown();
    }

    @Test
    void dlqIsEmptyForValidRecords() throws Exception {
        // publish a few valid records
        for (int i = 1; i <= 3; i++) {
            SimpleStringRecord rec = SimpleStringRecord.builder().id(String.valueOf(i)).value("ok-" + i).build();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key-" + i, rec)).get();
        }
        producer.flush();

        // wait for ingestion
        waitForDataInFirebolt(TABLE_NAME, 3);

        // poll DLQ briefly; should be empty
        ConsumerRecords<String, String> polled = dlqConsumer.poll(Duration.ofSeconds(5));
        assertEquals(0, polled.count(), "DLQ should be empty for valid records");
    }

    @Test
    void dlqReceivesMessagesWhenFireboleColumnConversionErrorHappens() throws Exception {
        SimpleStringRecord invalidRecord = SimpleStringRecord.builder().id("abc").value("failing").build();
        SimpleStringRecord validRecord = SimpleStringRecord.builder().id("abc").build();
        producer.send(new ProducerRecord<>(TOPIC_NAME, "invalidRecord", invalidRecord)).get();
        producer.send(new ProducerRecord<>(TOPIC_NAME, "validRecord", validRecord)).get();
        producer.flush();

        ConsumerRecords<String, String> polled = dlqConsumer.poll(Duration.ofSeconds(10));
        assertEquals(1, polled.count(), "Expected 1 DLQ message");
    }

    @Test
    @Tag(TestTag.CLOUD)
    void dlqReceivesErroredRecordsWhenPayloadTooBig() throws Exception {
        SimpleStringRecord huge = SimpleStringRecord.builder().id("Ā".repeat(2100000)).build();
        producer.send(new ProducerRecord<>(TOPIC_NAME, "huge", huge)).get();
        producer.flush();

        ConsumerRecords<String, String> polled = dlqConsumer.poll(Duration.ofSeconds(10));
        assertTrue(polled.count() > 0, "Expected some DLQ messages when failures occur");
    }

    private KafkaConsumer<String, String> createDlqConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // DLQ messages may not be plain strings; use byte[] to be robust
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return new KafkaConsumer<>(props);
    }

    private Supplier<String> tableSchemaSupplier() {
        return () -> "CREATE TABLE \"%s\" (\"id\" INTEGER NOT NULL, \"value\" TEXT NOT NULL)";
    }

    private Supplier<String> jsonSchemaSupplier() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"SimpleStringRecord\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"id\": { \"type\": \"string\" },\n" +
                "    \"value\": { \"type\": \"string\" }\n" +
                "  },\n" +
                "  \"required\": [\"id\", \"value\"]\n" +
                "}";
    }
}


