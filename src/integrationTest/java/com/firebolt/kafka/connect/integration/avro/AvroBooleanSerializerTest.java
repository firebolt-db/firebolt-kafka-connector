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
public class AvroBooleanSerializerTest extends AvroBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("boolean_test_table_avro");
    private String TOPIC_NAME = generateTopicName("boolean-test-topic-avro");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-boolean-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroBooleanSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro boolean data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                booleanTableSchema(), avroBooleanSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroBooleanSchema().get());
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

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        // Record 1: typical true values
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredBoolean", true);
        r1.put("optionalBoolean", false);
        r1.put("requiredListWithNullableElements", Arrays.asList(true, null, false, null, true));
        r1.put("requiredListWithNonNullElements", Arrays.asList(false, true, false, true, false));
        r1.put("optionalList", Arrays.asList(true, false, true));
        r1.put("optionalListWithNonNullElements", Arrays.asList(false, false, true));
        records.add(r1);

        // Record 2: false required, null optional
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredBoolean", false);
        r2.put("optionalBoolean", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(null, null, true, false));
        r2.put("requiredListWithNonNullElements", Arrays.asList(true, true, true, true));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        records.add(r2);

        // Record 3: empty lists
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredBoolean", true);
        r3.put("optionalBoolean", true);
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        records.add(r3);

        // Record 4: all-false arrays, various string representations
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredBoolean", false);
        r4.put("optionalBoolean", false);
        r4.put("requiredListWithNullableElements", Arrays.asList(false, false, null));
        r4.put("requiredListWithNonNullElements", Arrays.asList(false, false, false));
        r4.put("optionalList", Arrays.asList(false, true, null));
        r4.put("optionalListWithNonNullElements", Arrays.asList(true, false, true));
        records.add(r4);

        // Record 5: single-element arrays
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredBoolean", true);
        r5.put("optionalBoolean", null);
        r5.put("requiredListWithNullableElements", Arrays.asList(true, null));
        r5.put("requiredListWithNonNullElements", List.of(true));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        records.add(r5);

        // Record 6: "0" string for false
        GenericData.Record r6 = new GenericData.Record(schema);
        r6.put("recordId", 6);
        r6.put("requiredBoolean", false);
        r6.put("optionalBoolean", true);
        r6.put("requiredListWithNullableElements", Arrays.asList(null, true));
        r6.put("requiredListWithNonNullElements", List.of(false));
        r6.put("optionalList", Arrays.asList(true, false));
        r6.put("optionalListWithNonNullElements", List.of(true));
        records.add(r6);

        return records;
    }

    private Supplier<String> booleanTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredBoolean\" BOOLEAN NOT NULL, " +
                "\"optionalBoolean\" BOOLEAN NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(BOOLEAN NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(BOOLEAN NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(BOOLEAN NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(BOOLEAN NOT NULL) NULL" +
                ")";
    }

    private Supplier<String> avroBooleanSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"BooleanTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredBoolean\", \"type\": \"boolean\"},\n" +
                "    {\"name\": \"optionalBoolean\", \"type\": [\"null\", \"boolean\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"boolean\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"boolean\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"boolean\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"boolean\"}], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredBoolean\", \"optionalBoolean\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                int actualRecordId = rs.getInt("recordId");
                assertEquals(expected.get("recordId"), actualRecordId);
                assertEquals(expected.get("requiredBoolean"), rs.getBoolean("requiredBoolean"),
                        "requiredBoolean mismatch at index " + idx);

                Object expectedOptional = expected.get("optionalBoolean");
                Boolean actualOptional = rs.getObject("optionalBoolean") != null ? rs.getBoolean("optionalBoolean") : null;
                if (expectedOptional == null) {
                    assertNull(actualOptional, "optionalBoolean should be null at index " + idx);
                } else {
                    assertEquals(expectedOptional, actualOptional,
                            "optionalBoolean mismatch at index " + idx);
                }

                verifyBooleanArray("requiredListWithNullableElements",
                        (List<Boolean>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyBooleanArray("requiredListWithNonNullElements",
                        (List<Boolean>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyBooleanArray("optionalList",
                        (List<Boolean>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyBooleanArray("optionalListWithNonNullElements",
                        (List<Boolean>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyBooleanArray(String fieldName, List<Boolean> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.BOOLEAN, actualArray.getBaseType());
        Boolean[] elements = (Boolean[]) actualArray.getArray();
        assertEquals(expected, Arrays.asList(elements),
                fieldName + " mismatch at index " + idx);
    }
}
