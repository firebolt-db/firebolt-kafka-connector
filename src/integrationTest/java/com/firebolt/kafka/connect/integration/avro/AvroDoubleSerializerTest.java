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
public class AvroDoubleSerializerTest extends AvroBaseIntegrationTest {

    private static final String TABLE_NAME = "double_test_table_avro";
    private static final String TOPIC_NAME = "double-test-topic-avro";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-double-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroDoubleSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro double precision data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                doubleTableSchema(), avroDoubleSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroDoubleSchema().get());
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
        log.info("Running {} for Avro double invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                doubleTableSchema(), avroDoubleSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroDoubleSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "42.42");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "-17.5");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "abc");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "09-07-2025");

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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String doubleFromString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredDouble", 42.5);
        record.put("optionalDouble", 100.75);
        record.put("requiredListWithNullableElements", Arrays.asList(1.5, null, 3.25));
        record.put("requiredListWithNonNullElements", Arrays.asList(10.1, 20.2, 30.3));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("doubleFromString", doubleFromString);
        record.put("optionalInt", recordId * 10);
        record.put("optionalLong", (long) recordId * 100);
        record.put("optionalFloat", 12.5f);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredDouble", 42.5);
        r1.put("optionalDouble", 100.75);
        r1.put("requiredListWithNullableElements", Arrays.asList(1.5, null, 3.25, null, 5.75));
        r1.put("requiredListWithNonNullElements", Arrays.asList(10.1, 20.2, 30.3, 40.4, 50.5));
        r1.put("optionalList", Arrays.asList(100.1, 200.2, 300.3));
        r1.put("optionalListWithNonNullElements", Arrays.asList(111.1, 222.2, 333.3));
        r1.put("doubleFromString", "123.45");
        r1.put("optionalInt", 100);
        r1.put("optionalLong", 123456789012345L);
        r1.put("optionalFloat", 12.5f);
        records.add(r1);

        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredDouble", 123456789012345.123456789012345);
        r2.put("optionalDouble", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(
                null, null,
                Double.MAX_VALUE,
                -Double.MAX_VALUE));
        r2.put("requiredListWithNonNullElements", Arrays.asList(
                -Double.MAX_VALUE, 123.456789, -123.456789, Double.MAX_VALUE));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("doubleFromString", "-987654321098765.5");
        r2.put("optionalInt", null);
        r2.put("optionalLong", null);
        r2.put("optionalFloat", null);
        records.add(r2);

        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredDouble", Double.MIN_VALUE);
        r3.put("optionalDouble", Double.MIN_NORMAL);
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("doubleFromString", "0.000000000000001");
        r3.put("optionalInt", Integer.MAX_VALUE);
        r3.put("optionalLong", Long.MAX_VALUE);
        r3.put("optionalFloat", Float.MAX_VALUE);
        records.add(r3);

        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredDouble", 0.0);
        r4.put("optionalDouble", -0.0);
        r4.put("requiredListWithNullableElements", Arrays.asList(
                Math.PI, null, -Math.E, Double.MIN_NORMAL));
        r4.put("requiredListWithNonNullElements", Arrays.asList(
                Math.PI, Math.E, -Math.PI, -Math.E));
        r4.put("optionalList", Arrays.asList(-100.555, 200.666, null));
        r4.put("optionalListWithNonNullElements", Arrays.asList(19.99, 39.95, 59.00));
        r4.put("doubleFromString", "0");
        r4.put("optionalInt", Integer.MIN_VALUE);
        r4.put("optionalLong", Long.MIN_VALUE);
        r4.put("optionalFloat", -Float.MAX_VALUE);
        records.add(r4);

        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredDouble", 1.7976931348623157E+308);
        r5.put("optionalDouble", 4.9E-324);
        r5.put("requiredListWithNullableElements", Arrays.asList(
                Double.MIN_VALUE, Double.MAX_VALUE, null, Double.MIN_NORMAL));
        r5.put("requiredListWithNonNullElements", Arrays.asList(
                1.0E+15, -1.0E+15, 1.0E-15, -1.0E-15));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("doubleFromString", String.valueOf(Double.MAX_VALUE));
        r5.put("optionalInt", null);
        r5.put("optionalLong", 0L);
        r5.put("optionalFloat", Float.MIN_VALUE);
        records.add(r5);

        return records;
    }

    private Supplier<String> doubleTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredDouble\" DOUBLE PRECISION NOT NULL, " +
                "\"optionalDouble\" DOUBLE PRECISION NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(DOUBLE PRECISION NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(DOUBLE PRECISION NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(DOUBLE PRECISION NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(DOUBLE PRECISION NOT NULL) NULL, " +
                "\"doubleFromString\" DOUBLE PRECISION NOT NULL, " +
                "\"optionalInt\" DOUBLE PRECISION NULL, " +
                "\"optionalLong\" DOUBLE PRECISION NULL, " +
                "\"optionalFloat\" DOUBLE PRECISION NULL" +
                ")";
    }

    private Supplier<String> avroDoubleSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"DoubleTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredDouble\", \"type\": \"double\"},\n" +
                "    {\"name\": \"optionalDouble\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"double\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"double\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"double\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"double\"}], \"default\": null},\n" +
                "    {\"name\": \"doubleFromString\", \"type\": \"string\"},\n" +
                "    {\"name\": \"optionalInt\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"optionalLong\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
                "    {\"name\": \"optionalFloat\", \"type\": [\"null\", \"float\"], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredDouble\", \"optionalDouble\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", " +
                "\"doubleFromString\", \"optionalInt\", \"optionalLong\", \"optionalFloat\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                double expectedRequired = (Double) expected.get("requiredDouble");
                double actualRequired = rs.getDouble("requiredDouble");
                double tolerance = Math.max(1e-15, Math.abs(expectedRequired) * 1e-15);
                assertEquals(expectedRequired, actualRequired, tolerance,
                        "requiredDouble mismatch at index " + idx);

                Object expectedOptionalObj = expected.get("optionalDouble");
                Double actualOptional = rs.getObject("optionalDouble") != null ? rs.getDouble("optionalDouble") : null;
                if (expectedOptionalObj == null) {
                    assertNull(actualOptional, "optionalDouble should be null at index " + idx);
                } else {
                    double expectedOpt = (Double) expectedOptionalObj;
                    double tol = Math.max(1e-15, Math.abs(expectedOpt) * 1e-15);
                    assertEquals(expectedOpt, actualOptional, tol,
                            "optionalDouble mismatch at index " + idx);
                }

                verifyDoubleArray("requiredListWithNullableElements",
                        (List<Double>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyDoubleArray("requiredListWithNonNullElements",
                        (List<Double>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyDoubleArray("optionalList",
                        (List<Double>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyDoubleArray("optionalListWithNonNullElements",
                        (List<Double>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                double expectedFromString = Double.parseDouble(expected.get("doubleFromString").toString());
                double actualFromString = rs.getDouble("doubleFromString");
                double tolStr = Math.max(1e-15, Math.abs(expectedFromString) * 1e-15);
                assertEquals(expectedFromString, actualFromString, tolStr,
                        "doubleFromString mismatch at index " + idx);

                Object expectedOptionalInt = expected.get("optionalInt");
                Double actualOptionalInt = rs.getObject("optionalInt") != null ? rs.getDouble("optionalInt") : null;
                if (expectedOptionalInt == null) {
                    assertNull(actualOptionalInt, "optionalInt should be null at index " + idx);
                } else {
                    assertEquals(((Integer) expectedOptionalInt).doubleValue(), actualOptionalInt, 0.0,
                            "optionalInt mismatch at index " + idx);
                }

                Object expectedOptionalLong = expected.get("optionalLong");
                Double actualOptionalLong = rs.getObject("optionalLong") != null ? rs.getDouble("optionalLong") : null;
                if (expectedOptionalLong == null) {
                    assertNull(actualOptionalLong, "optionalLong should be null at index " + idx);
                } else {
                    assertEquals(((Long) expectedOptionalLong).doubleValue(), actualOptionalLong, 0.0,
                            "optionalLong mismatch at index " + idx);
                }

                Object expectedOptionalFloat = expected.get("optionalFloat");
                Double actualOptionalFloat = rs.getObject("optionalFloat") != null ? rs.getDouble("optionalFloat") : null;
                if (expectedOptionalFloat == null) {
                    assertNull(actualOptionalFloat, "optionalFloat should be null at index " + idx);
                } else {
                    double expectedFloatVal = ((Float) expectedOptionalFloat).doubleValue();
                    double tolFloat = Math.max(1e-7, Math.abs(expectedFloatVal) * 1e-7);
                    assertEquals(expectedFloatVal, actualOptionalFloat, tolFloat,
                            "optionalFloat mismatch at index " + idx);
                }

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyDoubleArray(String fieldName, List<Double> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.DOUBLE, actualArray.getBaseType());
        Double[] elements = (Double[]) actualArray.getArray();
        assertEquals(expected.size(), elements.length,
                fieldName + " size mismatch at index " + idx);
        for (int i = 0; i < expected.size(); i++) {
            Double expectedElement = expected.get(i);
            if (expectedElement == null) {
                assertNull(elements[i],
                        fieldName + " element " + i + " should be null at index " + idx);
            } else {
                assertNotNull(elements[i],
                        fieldName + " element " + i + " should not be null at index " + idx);
                double tol = Math.max(1e-15, Math.abs(expectedElement) * 1e-15);
                assertEquals(expectedElement, elements[i], tol,
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }
}
