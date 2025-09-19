package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.SimpleRecord;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class ConnectorManagementTest extends BaseIntegrationTest {

    private String topic1Name;
    private String topic2Name;

    private String table1Name;
    private String table2Name;

    private String schema1Subject;
    private String schema2Subject;

    private Producer<String, SimpleRecord> producer1;
    private Producer<String, SimpleRecord> producer2;

    @BeforeEach
    void setupTest(TestInfo testInfo) throws SQLException, IOException {
        super.setUp(testInfo);

        String methodName = testInfo.getTestMethod().get().getName();

        topic1Name = methodName + "1";
        topic2Name = methodName + "2";

        table1Name = topic1Name;
        table2Name = topic2Name;

        schema1Subject = topic1Name + "-value";
        schema2Subject = topic2Name + "-value";

        // Generate unique connector name for this test run
        generateUniqueConnectorName(methodName);

        // Setup test resources using centralized method
        setupTestResources(topic1Name, table1Name, schema1Subject,
                simpleRecordTableSchema(), jsonSimpleRecordSchema());

        // create the table2 table
        String createTableSql = String.format(simpleRecordTableSchema().get(), table2Name);
        fireboltDefaultDbClient.executeUpdate(createTableSql);

        // create topic2
        createKafkaTopic(topic2Name);

        // register schema for topic2
        getSchemaRegistryClient().registerSchema(schema2Subject, jsonSimpleRecordSchema().get(), "JSON");

        producer1 = initializeJsonProducer();
        producer2 = initializeJsonProducer();
    }

    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer1 != null) {
            producer1.close();
        }
        if (producer2 != null) {
            producer2.close();
        }

        // Clean up test resources for all topics
        cleanupTestResources(table1Name, topic1Name, schema1Subject);
        cleanupTestResources(table2Name, topic2Name, schema2Subject);

        super.tearDown();
    }

    @Test
    void canPauseChangeDefinitionAndResumeConnectorWithNewDefinition() throws Exception {
        List<SimpleRecord> topic1TestRecords = List.of(
                aValidTestRecord(1, "one"),
                aValidTestRecord(2, "two"),
                aValidTestRecord(3, "three")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1TestRecords);

        // messages should be in table1
        waitForDataInFirebolt(table1Name, topic1TestRecords.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, topic1TestRecords);

        kafkaConnectClient.pauseConnector(testConnectorName);

        // create messages for topic2
        List<SimpleRecord> topic2TestRecords = List.of(
                aValidTestRecord(6, "six"),
                aValidTestRecord(7, "seven"),
                aValidTestRecord(8, "eight"),
                aValidTestRecord(9, "nine")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2TestRecords);

        // there should be no messages in TABLE2
        waitForDataInFirebolt(table2Name, 0);

        Map<String, Object> connectorDefinition = kafkaConnectClient.getConnectorConfig(testConnectorName);
        assertEquals(topic1Name, connectorDefinition.get("topics"));
        assertEquals(topic1Name + ":" + table1Name, connectorDefinition.get("topic.to.table.mapping"));

        // change the definition to now listen to topic2 and to map topic2:table2
        connectorDefinition.put("topics", topic2Name);
        connectorDefinition.put("topic.to.table.mapping", topic2Name + ":" + table2Name);
        kafkaConnectClient.updateConnectorConfig(testConnectorName, connectorDefinition);

        kafkaConnectClient.resumeConnector(testConnectorName);
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        // publish one more message to topic1
        List<SimpleRecord> topic1NewRecords = List.of(
                aValidTestRecord(4, "four")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1NewRecords);

        // now we should have the table2 with new messages
        waitForDataInFirebolt(table2Name, topic2TestRecords.size());

        // check that all the records have the expected value
        verifyRecords(table2Name, topic2TestRecords);

        // table 1 should still have just the original records
        waitForDataInFirebolt(table1Name, topic1TestRecords.size());
    }

    @Test
    void canPauseDefinitionAddANewTopicToTableMapping() throws Exception {
        List<SimpleRecord> topic1TestRecords = List.of(
                aValidTestRecord(100, "one zero zero"),
                aValidTestRecord(101, "one zero one"),
                aValidTestRecord(102, "one zero two")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1TestRecords);

        // messages should be in table1
        waitForDataInFirebolt(table1Name, topic1TestRecords.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, topic1TestRecords);

        kafkaConnectClient.pauseConnector(testConnectorName);

        // create messages for topic2
        List<SimpleRecord> topic2TestRecords = List.of(
                aValidTestRecord(200, "two zero zero"),
                aValidTestRecord(201, "two zero one"),
                aValidTestRecord(202, "two zero two"),
                aValidTestRecord(203, "two zero three")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2TestRecords);

        // there should be no messages in TABLE2
        waitForDataInFirebolt(table2Name, 0);

        Map<String, Object> connectorDefinition = kafkaConnectClient.getConnectorConfig(testConnectorName);
        assertEquals(topic1Name, connectorDefinition.get("topics"));
        assertEquals(topic1Name + ":" + table1Name, connectorDefinition.get("topic.to.table.mapping"));

        // change the definition to now listen to topic2 and to map topic2:table2
        connectorDefinition.put("topics", topic1Name + "," + topic2Name);
        connectorDefinition.put("topic.to.table.mapping", topic1Name + ":" + table1Name + "," + topic2Name + ":" + table2Name);
        kafkaConnectClient.updateConnectorConfig(testConnectorName, connectorDefinition);

        // resume the connector
        kafkaConnectClient.resumeConnector(testConnectorName);

        // now we should have the table2
        waitForDataInFirebolt(table2Name, topic2TestRecords.size());

        // check that all the records have the expected value
        verifyRecords(table2Name, topic2TestRecords);

        // publish more data in both topic1 and topic2
        List<SimpleRecord> topic1NewTestRecords = List.of(
                aValidTestRecord(103, "one zero three"),
                aValidTestRecord(104, "one zero four")
        );
        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1NewTestRecords);

        // messages should be in table1
        List<SimpleRecord> expectedTable1Records = new ArrayList<>(topic1TestRecords);
        expectedTable1Records.addAll(topic1NewTestRecords);
        waitForDataInFirebolt(table1Name, expectedTable1Records.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, expectedTable1Records);

        // create messages for topic2
        List<SimpleRecord> topic2NewTestRecords = List.of(
                aValidTestRecord(204, "two zero four"),
                aValidTestRecord(205, "two zero five")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2NewTestRecords);

        List<SimpleRecord> expectedTable2Records = new ArrayList<>(topic2TestRecords);
        expectedTable2Records.addAll(topic2NewTestRecords);

        // these new messages should be in table 2
        waitForDataInFirebolt(table2Name, expectedTable2Records.size());

        verifyRecords(table2Name, expectedTable2Records);
    }

    @Test
    void restartingTheConnectorKeepsTheNewDefinition() throws Exception {
        // create
        Map<String, Object> connectorDefinition = kafkaConnectClient.getConnectorConfig(testConnectorName);
        assertEquals(topic1Name, connectorDefinition.get("topics"));
        assertEquals(topic1Name + ":" + table1Name, connectorDefinition.get("topic.to.table.mapping"));

        // change the definition to now listen to topic2 and to map topic2:table2
        connectorDefinition.put("topics", topic1Name + "," + topic2Name);
        connectorDefinition.put("topic.to.table.mapping", topic1Name + ":" + table1Name + "," + topic2Name + ":" + table2Name);
        kafkaConnectClient.updateConnectorConfig(testConnectorName, connectorDefinition);
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        kafkaConnectClient.restartConnector(testConnectorName);
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        List<SimpleRecord> topic1TestRecords = List.of(
                aValidTestRecord(300, "three zero zero"),
                aValidTestRecord(301, "three zero one"),
                aValidTestRecord(302, "three zero two")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1TestRecords);

        // messages should be in table1
        waitForDataInFirebolt(table1Name, topic1TestRecords.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, topic1TestRecords);

        // create messages for topic2
        List<SimpleRecord> topic2TestRecords = List.of(
                aValidTestRecord(400, "four zero zero"),
                aValidTestRecord(401, "four zero one"),
                aValidTestRecord(402, "four zero two"),
                aValidTestRecord(403, "four zero three")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2TestRecords);

        // there should be messages in TABLE2
        waitForDataInFirebolt(table2Name, topic2TestRecords.size());

        verifyRecords(table2Name, topic2TestRecords);

        connectorDefinition = kafkaConnectClient.getConnectorConfig(testConnectorName);

        // change the definition to only listen to topic1
        connectorDefinition.put("topics", topic1Name);
        connectorDefinition.put("topic.to.table.mapping", topic1Name + ":" + table1Name);
        kafkaConnectClient.updateConnectorConfig(testConnectorName, connectorDefinition);
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        // restart the connector
        kafkaConnectClient.restartConnector(testConnectorName);
        sleepForMillis(TimeUnit.SECONDS.toMillis(1));
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        List<SimpleRecord> topic1NewRecords = List.of(
                aValidTestRecord(303, "three zero three")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1NewRecords);

        List<SimpleRecord> expectedTable1Records = new ArrayList<>(topic1TestRecords);
        expectedTable1Records.addAll(topic1NewRecords);

        // now we should have the table1
        waitForDataInFirebolt(table1Name, expectedTable1Records.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, expectedTable1Records);

        // publish more data in topic2
        List<SimpleRecord> topic2NewTestRecords = List.of(
                aValidTestRecord(404, "four zero four")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2NewTestRecords);

        // sleep for 5 seconds to allow the eventual propagation of topic2 records
        sleepForMillis(TimeUnit.SECONDS.toMillis(5));

        // there should be no new messages in table2
        waitForDataInFirebolt(table2Name, topic2TestRecords.size());

        verifyRecords(table2Name, topic2TestRecords);
    }

    @Test
    void updatingTheConnectorDefinitionWhileRunningAppliesTheNewDefinition() throws Exception {
        // create
        Map<String, Object> connectorDefinition = kafkaConnectClient.getConnectorConfig(testConnectorName);
        assertEquals(topic1Name, connectorDefinition.get("topics"));
        assertEquals(topic1Name + ":" + table1Name, connectorDefinition.get("topic.to.table.mapping"));

        // change the definition to now listen to topic2 and to map topic2:table2
        connectorDefinition.put("topics", topic1Name + "," + topic2Name);
        connectorDefinition.put("topic.to.table.mapping", topic1Name + ":" + table1Name + "," + topic2Name + ":" + table2Name);
        kafkaConnectClient.updateConnectorConfig(testConnectorName, connectorDefinition);

        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        List<SimpleRecord> topic1TestRecords = List.of(
                aValidTestRecord(300, "three zero zero"),
                aValidTestRecord(301, "three zero one"),
                aValidTestRecord(302, "three zero two")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1TestRecords);

        // messages should be in table1
        waitForDataInFirebolt(table1Name, topic1TestRecords.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, topic1TestRecords);

        // create messages for topic2
        List<SimpleRecord> topic2TestRecords = List.of(
                aValidTestRecord(400, "four zero zero"),
                aValidTestRecord(401, "four zero one"),
                aValidTestRecord(402, "four zero two"),
                aValidTestRecord(403, "four zero three")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2TestRecords);

        // there should be messages in TABLE2
        waitForDataInFirebolt(table2Name, topic2TestRecords.size());

        verifyRecords(table2Name, topic2TestRecords);

        connectorDefinition = kafkaConnectClient.getConnectorConfig(testConnectorName);

        // change the definition to only listen to topic1
        connectorDefinition.put("topics", topic1Name);
        connectorDefinition.put("topic.to.table.mapping", topic1Name + ":" + table1Name);
        kafkaConnectClient.updateConnectorConfig(testConnectorName, connectorDefinition);
        kafkaConnectClient.waitForConnectorRunning(testConnectorName, Duration.ofSeconds(30));

        List<SimpleRecord> topic1NewRecords = List.of(
                aValidTestRecord(303, "three zero three")
        );

        // publish the messages to topic1
        publishMessages(producer1, topic1Name, topic1NewRecords);

        List<SimpleRecord> expectedTable1Records = new ArrayList<>(topic1TestRecords);
        expectedTable1Records.addAll(topic1NewRecords);

        // now we should have the table1
        waitForDataInFirebolt(table1Name, expectedTable1Records.size());

        // check that all the records have the expected value
        verifyRecords(table1Name, expectedTable1Records);

        // publish more data in topic2
        List<SimpleRecord> topic2NewTestRecords = List.of(
                aValidTestRecord(404, "four zero four")
        );

        // publish the messages to topic2
        publishMessages(producer2, topic2Name, topic2NewTestRecords);

        // sleep for 5 seconds to allow the eventual propagation of topic2 records
        sleepForMillis(TimeUnit.SECONDS.toMillis(5));

        // there should be no new messages in table2
        waitForDataInFirebolt(table2Name, topic2TestRecords.size());

        verifyRecords(table2Name, topic2TestRecords);
    }

    private void publishMessages(Producer<String, SimpleRecord> producer, String topicName, List<SimpleRecord> records) throws Exception {
        // Ensure the topic is fully ready before producing to avoid metadata timeouts
        waitForTopicReady(topicName, Duration.ofSeconds(60));

        for (SimpleRecord record : records) {
            String key = "large-message-test-key-" + record.getId();
            ProducerRecord<String, SimpleRecord> producerRecord =
                    new ProducerRecord<>(topicName, key, record);

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message with key {}: {}", key, exception.getMessage());
                } else {
                    log.debug("Successfully sent message with key {} to partition {} at offset {}",
                            key, metadata.partition(), metadata.offset());
                }
            }).get();
        }

        producer.flush();
    }

    private SimpleRecord aValidTestRecord(int recordId, String value) {
        return SimpleRecord.builder()
                .id(recordId)
                .value(value)
                .build();

    }
}
