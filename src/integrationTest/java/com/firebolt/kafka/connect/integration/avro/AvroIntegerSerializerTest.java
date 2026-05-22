package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.utils.TestTag;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class AvroIntegerSerializerTest extends AvroBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("integer_test_table_avro");
    private String TOPIC_NAME = generateTopicName("integer-test-topic-avro");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-integer-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroIntegerSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro integer data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                integerTableSchema(), avroIntegerSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroIntegerSchema().get());

        List<GenericData.Record> testRecords = createTestRecords(avroSchema);

        try (Producer<String, Object> producer = initializeAvroProducer()) {
            for (int i = 0; i < testRecords.size(); i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key-" + i, testRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        verifyRecordsInFirebolt(testRecords);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro integer invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                integerTableSchema(), avroIntegerSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroIntegerSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "201");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "202");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "abc");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "1.23");

        List<GenericData.Record> allRecords = List.of(valid1, invalid1, valid2, invalid2);
        List<GenericData.Record> expectedRecords = List.of(valid1, valid2);

        try (Producer<String, Object> producer = initializeAvroProducer()) {
            for (int i = 0; i < allRecords.size(); i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, "key-" + i, allRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());
        verifyRecordsInFirebolt(expectedRecords);
    }

    private GenericData.Record createValidRecord(Schema schema, int recordId, String integerFromString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredInteger", 42);
        record.put("optionalInteger", 100);
        record.put("requiredListWithNullableElements", Arrays.asList(1, null, 3));
        record.put("requiredListWithNonNullElements", Arrays.asList(10, 20, 30));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("integerFromString", integerFromString);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        // Record 1: typical values
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredInteger", 42);
        r1.put("optionalInteger", 100);
        r1.put("requiredListWithNullableElements", Arrays.asList(1, null, 3, null, 5));
        r1.put("requiredListWithNonNullElements", Arrays.asList(10, 20, 30, 40, 50));
        r1.put("optionalList", Arrays.asList(-100, 200, null));
        r1.put("optionalListWithNonNullElements", Arrays.asList(111, 222, 333));
        r1.put("integerFromString", "1");
        records.add(r1);

        // Record 2: minimum value
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredInteger", Integer.MIN_VALUE);
        r2.put("optionalInteger", 100);
        r2.put("requiredListWithNullableElements", Arrays.asList(1, 2, 3));
        r2.put("requiredListWithNonNullElements", Arrays.asList(10, 20, 30));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("integerFromString", String.valueOf(Integer.MIN_VALUE));
        records.add(r2);

        // Record 3: maximum value
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredInteger", Integer.MAX_VALUE);
        r3.put("optionalInteger", null);
        r3.put("requiredListWithNullableElements", Arrays.asList(null, null, Integer.MAX_VALUE, Integer.MIN_VALUE));
        r3.put("requiredListWithNonNullElements", Arrays.asList(Integer.MIN_VALUE, 123, -123, Integer.MAX_VALUE));
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("integerFromString", String.valueOf(Integer.MAX_VALUE));
        records.add(r3);

        // Record 4: empty required lists
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredInteger", 0);
        r4.put("optionalInteger", -999);
        r4.put("requiredListWithNullableElements", new ArrayList<>());
        r4.put("requiredListWithNonNullElements", new ArrayList<>());
        r4.put("optionalList", Arrays.asList(100, 200, 300));
        r4.put("optionalListWithNonNullElements", Arrays.asList(-100, 0, 200));
        r4.put("integerFromString", "0");
        records.add(r4);

        // Record 5: large lists
        List<Integer> largeNullable = new ArrayList<>();
        List<Integer> largeNonNull = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeNullable.add(i % 5 == 0 ? null : i);
            largeNonNull.add(i * 10);
        }
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredInteger", 999);
        r5.put("optionalInteger", -999);
        r5.put("requiredListWithNullableElements", largeNullable);
        r5.put("requiredListWithNonNullElements", largeNonNull);
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("integerFromString", "-12345");
        records.add(r5);

        return records;
    }

    private Supplier<String> integerTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredInteger\" INTEGER NOT NULL, " +
                "\"optionalInteger\" INTEGER NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(INTEGER NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(INTEGER NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(INTEGER NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(INTEGER NOT NULL) NULL, " +
                "\"integerFromString\" INTEGER NOT NULL" +
                ")";
    }

    private Supplier<String> avroIntegerSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"IntegerTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredInteger\", \"type\": \"int\"},\n" +
                "    {\"name\": \"optionalInteger\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"int\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"int\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"int\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"int\"}], \"default\": null},\n" +
                "    {\"name\": \"integerFromString\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredInteger\", \"optionalInteger\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"integerFromString\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));
                assertEquals(expected.get("requiredInteger"), rs.getInt("requiredInteger"));

                Object expectedOptional = expected.get("optionalInteger");
                if (expectedOptional == null) {
                    assertNull(rs.getObject("optionalInteger"));
                } else {
                    assertEquals(expectedOptional, rs.getInt("optionalInteger"));
                }

                verifyIntegerArray("requiredListWithNullableElements",
                        (List<Integer>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyIntegerArray("requiredListWithNonNullElements",
                        (List<Integer>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyIntegerArray("optionalList",
                        (List<Integer>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyIntegerArray("optionalListWithNonNullElements",
                        (List<Integer>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                int expectedIntegerFromString = Integer.parseInt(expected.get("integerFromString").toString());
                assertEquals(expectedIntegerFromString, rs.getInt("integerFromString"),
                        "integerFromString mismatch at index " + idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyIntegerArray(String fieldName, List<Integer> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.INTEGER, actualArray.getBaseType());
        Integer[] elements = (Integer[]) actualArray.getArray();
        assertEquals(expected, Arrays.asList(elements), fieldName + " mismatch at index " + idx);
    }
}
