package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.utils.TestTag;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class AvroJsonSerializerTest extends AvroBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("json_test_table_avro");
    private String TOPIC_NAME = generateTopicName("json-test-topic-avro");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-json-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroJsonAsStringSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro JSON data type (string)", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                jsonTableSchema(), avroJsonStringSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroJsonStringSchema().get());
        List<GenericData.Record> testRecords = createJsonStringTestRecords(avroSchema);

        try (Producer<String, Object> producer = initializeAvroProducer()) {
            for (int i = 0; i < testRecords.size(); i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key-" + i, testRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        verifyJsonStringRecordsInFirebolt(testRecords);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    @Disabled("pending engine struct->json assignment cast")
    void testAvroJsonAsNestedRecordSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro JSON data type (nested record)", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                jsonTableSchema(), avroJsonNestedRecordSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroJsonNestedRecordSchema().get());
        List<GenericData.Record> testRecords = createJsonNestedRecordTestRecords(avroSchema);

        try (Producer<String, Object> producer = initializeAvroProducer()) {
            for (int i = 0; i < testRecords.size(); i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key-" + i, testRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        verifyJsonNestedRecordInFirebolt(testRecords);
    }

    /**
     * Creates all 21 test records matching JsonSchemaSerializerTest.createAllValidJsonValueRecords().
     * For Avro string type, we store the string as-is, so expected equals requiredJson.
     */
    private List<GenericData.Record> createJsonStringTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        records.add(createJsonStringRecord(schema, 1, "null"));
        records.add(createJsonStringRecord(schema, 2, "true"));
        records.add(createJsonStringRecord(schema, 3, "false"));
        records.add(createJsonStringRecord(schema, 5, "0"));
        records.add(createJsonStringRecord(schema, 6, "128"));
        records.add(createJsonStringRecord(schema, 7, "32768"));
        records.add(createJsonStringRecord(schema, 8, "2147483648"));
        records.add(createJsonStringRecord(schema, 9, "9223372036854775808"));
        records.add(createJsonStringRecord(schema, 10, "18446744073709551615"));
        records.add(createJsonStringRecord(schema, 11, "3.1415926"));
        records.add(createJsonStringRecord(schema, 12, "\"Hello world!\""));
        records.add(createJsonStringRecord(schema, 13, "\"Hello UTF-8! :fire:\""));
        records.add(createJsonStringRecord(schema, 14, "[]"));
        records.add(createJsonStringRecord(schema, 15, "[1,2,3,4]"));
        records.add(createJsonStringRecord(schema, 16, "{}"));
        records.add(createJsonStringRecord(schema, 17, "{\"a\":{\"b\":\"c\"}}"));
        records.add(createJsonStringRecord(schema, 18, "[{}]"));
        records.add(createJsonStringRecord(schema, 19, "[{\"a\":[\"b\",{\"c\":\"d\"}]},[\"e\",\"f\",{\"g\":[\"h\",\"i\"]}]]"));
        records.add(createJsonStringRecord(schema, 20, "{\"nested\":{\":droplet:\":\"water\",\":ice_cube:\":\"ice\"},\":fire:\":\":fire_extinguisher:\"}"));
        records.add(createJsonStringRecord(schema, 21, null));

        return records;
    }

    private GenericData.Record createJsonStringRecord(Schema schema, int recordId, String jsonValue) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredJson", jsonValue);
        return record;
    }

    private List<GenericData.Record> createJsonNestedRecordTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        Schema requiredJsonUnion = schema.getField("requiredJson").schema();
        Schema nestedJsonSchema = requiredJsonUnion.getTypes().get(1);

        // Record with simple nested object
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        GenericData.Record nested1 = new GenericData.Record(nestedJsonSchema);
        nested1.put("name", "test");
        nested1.put("value", 42);
        nested1.put("inner", null);
        r1.put("requiredJson", nested1);
        records.add(r1);

        // Record with nested object containing another nested object
        Schema innerJsonSchema = nestedJsonSchema.getField("inner").schema().getTypes().get(1);
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        GenericData.Record inner = new GenericData.Record(innerJsonSchema);
        inner.put("key", "nested");
        inner.put("data", 123);
        GenericData.Record nested2 = new GenericData.Record(nestedJsonSchema);
        nested2.put("name", "complex");
        nested2.put("value", 100);
        nested2.put("inner", inner);
        r2.put("requiredJson", nested2);
        records.add(r2);

        return records;
    }

    private Supplier<String> jsonTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredJson\" JSON NULL " +
                ")";
    }

    private Supplier<String> avroJsonStringSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"JsonStringTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredJson\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    private Supplier<String> avroJsonNestedRecordSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"JsonNestedTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\n" +
                "      \"name\": \"requiredJson\",\n" +
                "      \"type\": [\"null\", {\n" +
                "        \"type\": \"record\",\n" +
                "        \"name\": \"NestedJson\",\n" +
                "        \"fields\": [\n" +
                "          {\"name\": \"name\", \"type\": \"string\"},\n" +
                "          {\"name\": \"value\", \"type\": \"int\"},\n" +
                "          {\n" +
                "            \"name\": \"inner\",\n" +
                "            \"type\": [\"null\", {\n" +
                "              \"type\": \"record\",\n" +
                "              \"name\": \"InnerJson\",\n" +
                "              \"fields\": [\n" +
                "                {\"name\": \"key\", \"type\": \"string\"},\n" +
                "                {\"name\": \"data\", \"type\": \"int\"}\n" +
                "              ]\n" +
                "            }],\n" +
                "            \"default\": null\n" +
                "          }\n" +
                "        ]\n" +
                "      }],\n" +
                "      \"default\": null\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }

    /**
     * Returns expected JSON for verification. Matches JsonSchemaSerializerTest expectedRequiredJson exactly.
     */
    private String getExpectedJsonForVerification(int recordId) {
        switch (recordId) {
            case 1:
                return "null";
            case 2:
                return "true";
            case 3:
                return "false";
            case 5:
                return "0";
            case 6:
                return "128";
            case 7:
                return "32768";
            case 8:
                return "\"2147483648\"";
            case 9:
                return "\"9223372036854775808\"";
            case 10:
                return "\"18446744073709551615\"";
            case 11:
                return "3.1415926";
            case 12:
                return "\"Hello world!\"";
            case 13:
                return "\"Hello UTF-8! :fire:\"";
            case 14:
                return "[]";
            case 15:
                return "[1,2,3,4]";
            case 16:
                return "{}";
            case 17:
                return "{\"a\":{\"b\":\"c\"}}";
            case 18:
                return "[{}]";
            case 19:
                return "[{\"a\":[\"b\",{\"c\":\"d\"}]},[\"e\",\"f\",{\"g\":[\"h\",\"i\"]}]]";
            case 20:
                return "{\":fire:\":\":fire_extinguisher:\",\"nested\":{\":droplet:\":\"water\",\":ice_cube:\":\"ice\"}}";
            case 21:
                return null;
            default:
                throw new IllegalArgumentException("Unknown recordId: " + recordId);
        }
    }

    /**
     * Verification matches JsonSchemaSerializerTest.verifyJsonRecordsInFirebolt.
     */
    private void verifyJsonStringRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredJson\" FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size(),
                        "More records found in database than expected");
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"),
                        "recordId mismatch at index " + idx);

                String expectedStr = getExpectedJsonForVerification((Integer) expected.get("recordId"));
                String actualJson = rs.getString("requiredJson");
                assertEquals(expectedStr, actualJson,
                        "requiredJson mismatch at index " + idx);
                idx++;
            }
            assertEquals(expectedRecords.size(), idx,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + idx);
        }
    }

    private void verifyJsonNestedRecordInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredJson\" FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                String actualJson = rs.getString("requiredJson");
                assertTrue(actualJson != null && !actualJson.isEmpty(),
                        "requiredJson should not be null or empty at index " + idx);

                // Verify the JSON contains expected structure (order may vary)
                if (idx == 0) {
                    assertTrue(actualJson.contains("\"name\"") && actualJson.contains("\"test\""),
                            "Expected name and test in JSON: " + actualJson);
                    assertTrue(actualJson.contains("\"value\"") && actualJson.contains("42"),
                            "Expected value and 42 in JSON: " + actualJson);
                } else if (idx == 1) {
                    assertTrue(actualJson.contains("\"name\"") && actualJson.contains("\"complex\""),
                            "Expected name and complex in JSON: " + actualJson);
                    assertTrue(actualJson.contains("\"inner\"") && actualJson.contains("\"key\""),
                            "Expected inner object in JSON: " + actualJson);
                }
                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }
}
