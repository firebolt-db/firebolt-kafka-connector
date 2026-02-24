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
public class AvroTextSerializerTest extends AvroBaseIntegrationTest {

    private static final String TABLE_NAME = "text_test_table_avro";
    private static final String TOPIC_NAME = "text-test-topic-avro";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-text-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroTextSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro text data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                textTableSchema(), avroTextSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroTextSchema().get());
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
    void testAvroTextWithUnicodeAndSpecialCharacters(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro text unicode and special chars", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                textTableSchema(), avroTextSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroTextSchema().get());

        GenericData.Record r1 = new GenericData.Record(avroSchema);
        r1.put("recordId", 1);
        r1.put("requiredText", "Hello 世界! 🌍 Ñiño français العربية русский 日本語");
        r1.put("optionalText", "🚀✨🎯💯🔥⭐️🎉💎🌟⚡️");
        r1.put("requiredListWithNullableElements", Arrays.asList("Hello", null, "世界", "🌍", null, "日本語"));
        r1.put("requiredListWithNonNullElements", Arrays.asList("non-null", "unicode", "🚀", "✨"));
        r1.put("optionalList", Arrays.asList("optional", null, "unicode", "🎯"));
        r1.put("optionalListWithNonNullElements", Arrays.asList("non-null", "optional", "unicode", "💯"));

        List<GenericData.Record> testRecords = List.of(r1);

        try (Producer<String, Object> producer = initializeAvroProducer()) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key-0", r1)).get();
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        verifyRecordsInFirebolt(testRecords);
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        // Record 1: typical values
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredText", "Default required text");
        r1.put("optionalText", "Default optional text");
        r1.put("requiredListWithNullableElements", Arrays.asList("first", null, "third", null, "fifth"));
        r1.put("requiredListWithNonNullElements", Arrays.asList("non-null1", "non-null2", "non-null3", "non-null4", "non-null5"));
        r1.put("optionalList", Arrays.asList("optional1", "optional2", "optional3"));
        r1.put("optionalListWithNonNullElements", Arrays.asList("non-null-opt1", "non-null-opt2", "non-null-opt3"));
        records.add(r1);

        // Record 2: empty strings
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredText", "");
        r2.put("optionalText", "");
        r2.put("requiredListWithNullableElements", Arrays.asList("", null, ""));
        r2.put("requiredListWithNonNullElements", Arrays.asList("", ""));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        records.add(r2);

        // Record 3: null optional text
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredText", "Required only");
        r3.put("optionalText", null);
        r3.put("requiredListWithNullableElements", Arrays.asList("a", null, "b"));
        r3.put("requiredListWithNonNullElements", Arrays.asList("x", "y", "z"));
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        records.add(r3);

        // Record 4: empty lists
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredText", "Empty lists test");
        r4.put("optionalText", "Optional");
        r4.put("requiredListWithNullableElements", new ArrayList<>());
        r4.put("requiredListWithNonNullElements", new ArrayList<>());
        r4.put("optionalList", new ArrayList<>());
        r4.put("optionalListWithNonNullElements", new ArrayList<>());
        records.add(r4);

        // Record 5: null optional list
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredText", "Null optional list");
        r5.put("optionalText", "Text");
        r5.put("requiredListWithNullableElements", Arrays.asList("one", "two"));
        r5.put("requiredListWithNonNullElements", Arrays.asList("a", "b"));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        records.add(r5);

        return records;
    }

    private Supplier<String> textTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredText\" TEXT NOT NULL, " +
                "\"optionalText\" TEXT NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TEXT NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TEXT NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TEXT NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TEXT NOT NULL) NULL" +
                ")";
    }

    private Supplier<String> avroTextSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TextTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredText\", \"type\": \"string\"},\n" +
                "    {\"name\": \"optionalText\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"string\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"string\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredText\", \"optionalText\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));
                assertEquals(expected.get("requiredText").toString(), rs.getString("requiredText"),
                        "requiredText mismatch at index " + idx);

                Object expectedOptional = expected.get("optionalText");
                if (expectedOptional == null) {
                    assertNull(rs.getString("optionalText"),
                            "optionalText should be null at index " + idx);
                } else {
                    assertEquals(expectedOptional.toString(), rs.getString("optionalText"),
                            "optionalText mismatch at index " + idx);
                }

                verifyTextArray("requiredListWithNullableElements",
                        (List<Object>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyTextArray("requiredListWithNonNullElements",
                        (List<Object>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyTextArray("optionalList",
                        (List<Object>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyTextArray("optionalListWithNonNullElements",
                        (List<Object>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyTextArray(String fieldName, List<Object> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.VARCHAR, actualArray.getBaseType());
        String[] elements = (String[]) actualArray.getArray();
        assertEquals(expected.size(), elements.length,
                fieldName + " size mismatch at index " + idx);
        for (int i = 0; i < expected.size(); i++) {
            Object expectedObj = expected.get(i);
            if (expectedObj == null) {
                assertNull(elements[i],
                        fieldName + " element " + i + " should be null at index " + idx);
            } else {
                assertNotNull(elements[i],
                        fieldName + " element " + i + " should not be null at index " + idx);
                assertEquals(expectedObj.toString(), elements[i],
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }
}
