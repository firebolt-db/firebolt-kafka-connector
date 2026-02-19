package com.firebolt.kafka.connect.integration.avro;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class AvroRealSerializerTest extends AvroBaseIntegrationTest {

    private static final String TABLE_NAME = "real_test_table_avro";
    private static final String TOPIC_NAME = "real-test-topic-avro";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-real-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroRealSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro real data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                realTableSchema(), avroRealSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroRealSchema().get());
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
        log.info("Running {} for Avro real invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                realTableSchema(), avroRealSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroRealSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "12.34");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "-0.56");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "abc");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "12.34.56");

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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String realFromString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredReal", 299.95f);
        record.put("optionalReal", 1234.56f);
        record.put("requiredListWithNullableElements", Arrays.asList(15.75f, null, 89.25f));
        record.put("requiredListWithNonNullElements", Arrays.asList(23.45f, 67.89f, 134.12f));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("realFromString", realFromString);
        record.put("optionalInt", recordId * 10);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredReal", 299.95f);
        r1.put("optionalReal", 1234.56f);
        r1.put("requiredListWithNullableElements", Arrays.asList(15.75f, null, 89.25f, null, 156.50f));
        r1.put("requiredListWithNonNullElements", Arrays.asList(23.45f, 67.89f, 134.12f, 256.78f, 398.99f));
        r1.put("optionalList", Arrays.asList(499.99f, 799.50f, 1299.75f));
        r1.put("optionalListWithNonNullElements", Arrays.asList(78.33f, 145.67f, 289.44f));
        r1.put("realFromString", "123.45");
        r1.put("optionalInt", 100);
        records.add(r1);

        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredReal", -999999.99f);
        r2.put("optionalReal", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(null, null, Float.MAX_VALUE, -Float.MAX_VALUE));
        r2.put("requiredListWithNonNullElements", Arrays.asList(-Float.MAX_VALUE, 123.456f, -123.456f, Float.MAX_VALUE));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("realFromString", "-999999.99");
        r2.put("optionalInt", null);
        records.add(r2);

        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredReal", Float.MIN_VALUE);
        r3.put("optionalReal", 0.000001f);
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("realFromString", "0.000001");
        r3.put("optionalInt", Integer.MAX_VALUE);
        records.add(r3);

        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredReal", 0.0f);
        r4.put("optionalReal", -0.0f);
        r4.put("requiredListWithNullableElements", Arrays.asList(1.23e6f, null, -4.56e-3f));
        r4.put("requiredListWithNonNullElements", Arrays.asList(3.14159f, 2.71828f, 1.41421f));
        r4.put("optionalList", Arrays.asList(-100.555f, 200.666f, null));
        r4.put("optionalListWithNonNullElements", Arrays.asList(19.99f, 39.95f, 59.00f));
        r4.put("realFromString", "0");
        r4.put("optionalInt", Integer.MIN_VALUE);
        records.add(r4);

        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredReal", Float.MAX_VALUE);
        r5.put("optionalReal", -Float.MAX_VALUE);
        r5.put("requiredListWithNullableElements", Arrays.asList(Float.MIN_VALUE, null));
        r5.put("requiredListWithNonNullElements", Arrays.asList(Float.MIN_VALUE));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("realFromString", String.valueOf(Float.MAX_VALUE));
        r5.put("optionalInt", null);
        records.add(r5);

        return records;
    }

    private Supplier<String> realTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredReal\" REAL NOT NULL, " +
                "\"optionalReal\" REAL NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(REAL NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(REAL NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(REAL NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(REAL NOT NULL) NULL, " +
                "\"realFromString\" REAL NOT NULL, " +
                "\"optionalInt\" REAL NULL" +
                ")";
    }

    private Supplier<String> avroRealSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"RealTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredReal\", \"type\": \"float\"},\n" +
                "    {\"name\": \"optionalReal\", \"type\": [\"null\", \"float\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"float\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"float\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"float\"}], \"default\": null},\n" +
                "    {\"name\": \"realFromString\", \"type\": \"string\"},\n" +
                "    {\"name\": \"optionalInt\", \"type\": [\"null\", \"int\"], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredReal\", \"optionalReal\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"realFromString\", \"optionalInt\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));
                assertEquals((Float) expected.get("requiredReal"), rs.getFloat("requiredReal"), 0.0001f,
                        "requiredReal mismatch at index " + idx);

                Object expectedOptional = expected.get("optionalReal");
                if (expectedOptional == null) {
                    assertNull(rs.getObject("optionalReal"),
                            "optionalReal should be null at index " + idx);
                } else {
                    assertEquals((Float) expectedOptional, rs.getFloat("optionalReal"), 0.0001f,
                            "optionalReal mismatch at index " + idx);
                }

                verifyFloatArray("requiredListWithNullableElements",
                        (List<Float>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyFloatArray("requiredListWithNonNullElements",
                        (List<Float>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyFloatArray("optionalList",
                        (List<Float>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyFloatArray("optionalListWithNonNullElements",
                        (List<Float>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                float expectedFromString = Float.parseFloat(expected.get("realFromString").toString());
                assertEquals(expectedFromString, rs.getFloat("realFromString"), 0.0001f,
                        "realFromString mismatch at index " + idx);

                Object expectedOptionalInt = expected.get("optionalInt");
                if (expectedOptionalInt == null) {
                    assertNull(rs.getObject("optionalInt"),
                            "optionalInt should be null at index " + idx);
                } else {
                    assertEquals(((Integer) expectedOptionalInt).floatValue(),
                            rs.getFloat("optionalInt"), 0.0001f,
                            "optionalInt mismatch at index " + idx);
                }

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyFloatArray(String fieldName, List<Float> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.REAL, actualArray.getBaseType());
        Float[] elements = (Float[]) actualArray.getArray();
        assertEquals(expected.size(), elements.length,
                fieldName + " size mismatch at index " + idx);
        for (int i = 0; i < expected.size(); i++) {
            Float expectedElement = expected.get(i);
            if (expectedElement == null) {
                assertNull(elements[i],
                        fieldName + " element " + i + " should be null at index " + idx);
            } else {
                assertNotNull(elements[i],
                        fieldName + " element " + i + " should not be null at index " + idx);
                assertEquals(expectedElement, elements[i], 0.0001f,
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }
}
