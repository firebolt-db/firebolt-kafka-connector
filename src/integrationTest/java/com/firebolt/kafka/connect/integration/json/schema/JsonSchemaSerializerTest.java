package com.firebolt.kafka.connect.integration.json.schema;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.JsonRecord;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class JsonSchemaSerializerTest extends SchemaBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("json_test_table");
    private String TOPIC_NAME = generateTopicName("json-test-topic");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, JsonRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("json-serializer-test");
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }
        cleanupTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("ingestionTypesWithOrWithoutNulls")
    void testJsonSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for JSON data type (schema)", testDescription);

        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                jsonTableSchema(), jsonSchemaDefinition(), connectorOverrides);

        producer = initializeJsonProducer(includeNulls);

        List<JsonRecord> testRecords = createAllValidJsonValueRecords();

        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        verifyJsonRecordsInFirebolt(testRecords);
    }

    private Supplier<String> jsonTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredJson\" JSON NULL " +
                ")";
    }

    private Supplier<String> jsonSchemaDefinition() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"JSON Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredJson\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"JSON field stored as string - can contain any valid JSON\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\"]\n" +
                "}";
    }

    private List<JsonRecord> createAllValidJsonValueRecords() {
        return Arrays.asList(
                JsonRecord.builder().recordId(1).requiredJson("null").expectedRequiredJson("null").build(),
                JsonRecord.builder().recordId(2).requiredJson("true").expectedRequiredJson("true").build(),
                JsonRecord.builder().recordId(3).requiredJson("false").expectedRequiredJson("false").build(),
                JsonRecord.builder().recordId(5).requiredJson("0").expectedRequiredJson("0").build(),
                JsonRecord.builder().recordId(6).requiredJson("128").expectedRequiredJson("128").build(),
                JsonRecord.builder().recordId(7).requiredJson("32768").expectedRequiredJson("32768").build(),
                JsonRecord.builder().recordId(8).requiredJson("2147483648").expectedRequiredJson("\"2147483648\"").build(),
                JsonRecord.builder().recordId(9).requiredJson("9223372036854775808").expectedRequiredJson("\"9223372036854775808\"").build(),
                JsonRecord.builder().recordId(10).requiredJson("18446744073709551615").expectedRequiredJson("\"18446744073709551615\"").build(),
                JsonRecord.builder().recordId(11).requiredJson("3.1415926").expectedRequiredJson("3.1415926").build(),
                JsonRecord.builder().recordId(12).requiredJson("\"Hello world!\"").expectedRequiredJson("\"Hello world!\"").build(),
                JsonRecord.builder().recordId(13).requiredJson("\"Hello UTF-8! :fire:\"").expectedRequiredJson("\"Hello UTF-8! :fire:\"").build(),
                JsonRecord.builder().recordId(14).requiredJson("[]").expectedRequiredJson("[]").build(),
                JsonRecord.builder().recordId(15).requiredJson("[1,2,3,4]").expectedRequiredJson("[1,2,3,4]").build(),
                JsonRecord.builder().recordId(16).requiredJson("{}").expectedRequiredJson("{}").build(),
                JsonRecord.builder().recordId(17).requiredJson("{\"a\":{\"b\":\"c\"}}").expectedRequiredJson("{\"a\":{\"b\":\"c\"}}").build(),
                JsonRecord.builder().recordId(18).requiredJson("[{}]").expectedRequiredJson("[{}]").build(),
                JsonRecord.builder().recordId(19).requiredJson("[{\"a\":[\"b\",{\"c\":\"d\"}]},[\"e\",\"f\",{\"g\":[\"h\",\"i\"]}]]")
                        .expectedRequiredJson("[{\"a\":[\"b\",{\"c\":\"d\"}]},[\"e\",\"f\",{\"g\":[\"h\",\"i\"]}]]").build(),
                JsonRecord.builder().recordId(20).requiredJson("{\"nested\":{\":droplet:\":\"water\",\":ice_cube:\":\"ice\"},\":fire:\":\":fire_extinguisher:\"}")
                        .expectedRequiredJson("{\":fire:\":\":fire_extinguisher:\",\"nested\":{\":droplet:\":\"water\",\":ice_cube:\":\"ice\"}}").build(),
                JsonRecord.builder().recordId(21).requiredJson(null).expectedRequiredJson(null).build()
        );
    }

    private void publishMessages(List<JsonRecord> records) throws Exception {
        for (JsonRecord record : records) {
            String key = "json-test-key-" + record.getRecordId();
            ProducerRecord<String, JsonRecord> producerRecord =
                    new ProducerRecord<>(TOPIC_NAME, key, record);
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

    private void verifyJsonRecordsInFirebolt(List<JsonRecord> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredJson\" " +
                        "FROM \"%s\" as t ORDER BY \"recordId\"",
                TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                JsonRecord expected = expectedRecords.get(recordIndex);
                assertEquals(expected.getRecordId(), rs.getInt("recordId"));
                assertEquals(expected.getExpectedRequiredJson(), rs.getString("requiredJson"));

                recordIndex++;
            }
            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

}
