package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.utils.TestTag;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class AvroByteaSerializerTest extends AvroBaseIntegrationTest {

    private static final String TABLE_NAME = "bytea_test_table_avro";
    private static final String TOPIC_NAME = "bytea-test-topic-avro";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-bytea-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroByteaSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro bytea data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                byteaTableSchema(), avroByteaSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroByteaSchema().get());
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

        // Record 1: typical UTF-8 text as binary
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredBytea", ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8)));
        r1.put("optionalBytea", ByteBuffer.wrap("Optional binary data".getBytes(StandardCharsets.UTF_8)));
        r1.put("requiredListWithNullableElements", Arrays.asList(
                ByteBuffer.wrap("list-item-1".getBytes(StandardCharsets.UTF_8)),
                null,
                ByteBuffer.wrap("list-item-3".getBytes(StandardCharsets.UTF_8)),
                null,
                ByteBuffer.wrap("list-item-5".getBytes(StandardCharsets.UTF_8))));
        r1.put("requiredListWithNonNullElements", Arrays.asList(
                ByteBuffer.wrap("non-null-1".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("non-null-2".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("non-null-3".getBytes(StandardCharsets.UTF_8))));
        r1.put("optionalList", Arrays.asList(
                ByteBuffer.wrap("opt-1".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("opt-2".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("opt-3".getBytes(StandardCharsets.UTF_8))));
        r1.put("optionalListWithNonNullElements", Arrays.asList(
                ByteBuffer.wrap("choice-1".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("choice-2".getBytes(StandardCharsets.UTF_8))));
        r1.put("byteaAsString", "SGVsbG8gV29ybGQ=");
        records.add(r1);

        // Record 2: single byte values, null optional
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredBytea", ByteBuffer.wrap(new byte[]{0x01}));
        r2.put("optionalBytea", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(
                null, null,
                ByteBuffer.wrap(new byte[]{0x0A}),
                ByteBuffer.wrap(new byte[]{0x0B})));
        r2.put("requiredListWithNonNullElements", Arrays.asList(
                ByteBuffer.wrap(new byte[]{0x10}),
                ByteBuffer.wrap(new byte[]{0x20}),
                ByteBuffer.wrap(new byte[]{0x30})));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("byteaAsString", "AQ==");
        records.add(r2);

        // Record 3: empty lists
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredBytea", ByteBuffer.wrap("Non-empty required".getBytes(StandardCharsets.UTF_8)));
        r3.put("optionalBytea", ByteBuffer.wrap("Has optional".getBytes(StandardCharsets.UTF_8)));
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("byteaAsString", "Tm9uLWVtcHR5IHJlcXVpcmVk");
        records.add(r3);

        // Record 4: unicode content as binary
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredBytea", ByteBuffer.wrap("Hello 世界".getBytes(StandardCharsets.UTF_8)));
        r4.put("optionalBytea", ByteBuffer.wrap("Ünïcödë".getBytes(StandardCharsets.UTF_8)));
        r4.put("requiredListWithNullableElements", Arrays.asList(
                ByteBuffer.wrap("日本語".getBytes(StandardCharsets.UTF_8)),
                null,
                ByteBuffer.wrap("한국어".getBytes(StandardCharsets.UTF_8))));
        r4.put("requiredListWithNonNullElements", Arrays.asList(
                ByteBuffer.wrap("αβγ".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("δεζ".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("ηθι".getBytes(StandardCharsets.UTF_8))));
        r4.put("optionalList", Arrays.asList(
                null,
                ByteBuffer.wrap("café".getBytes(StandardCharsets.UTF_8)),
                null,
                ByteBuffer.wrap("naïve".getBytes(StandardCharsets.UTF_8))));
        r4.put("optionalListWithNonNullElements", Arrays.asList(
                ByteBuffer.wrap("résumé".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("über".getBytes(StandardCharsets.UTF_8))));
        r4.put("byteaAsString", "SGVsbG8g5LiW55WM");
        records.add(r4);

        // Record 5: special characters and larger data
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredBytea", ByteBuffer.wrap("Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?".getBytes(StandardCharsets.UTF_8)));
        r5.put("optionalBytea", null);
        r5.put("requiredListWithNullableElements", Arrays.asList(
                ByteBuffer.wrap("tab\there".getBytes(StandardCharsets.UTF_8)),
                null,
                ByteBuffer.wrap("newline\nhere".getBytes(StandardCharsets.UTF_8))));
        r5.put("requiredListWithNonNullElements", List.of(
                ByteBuffer.wrap("single-element".getBytes(StandardCharsets.UTF_8))));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("byteaAsString", "U3BlY2lhbCBjaGFycw==");
        records.add(r5);

        return records;
    }

    private Supplier<String> byteaTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredBytea\" BYTEA NOT NULL, " +
                "\"optionalBytea\" BYTEA NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(BYTEA NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(BYTEA NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(BYTEA NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(BYTEA NOT NULL) NULL, " +
                "\"byteaAsString\" BYTEA NOT NULL" +
                ")";
    }

    private Supplier<String> avroByteaSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"ByteaTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredBytea\", \"type\": \"bytes\"},\n" +
                "    {\"name\": \"optionalBytea\", \"type\": [\"null\", \"bytes\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"bytes\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"bytes\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"bytes\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"bytes\"}], \"default\": null},\n" +
                "    {\"name\": \"byteaAsString\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredBytea\", \"optionalBytea\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"byteaAsString\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                byte[] expectedRequired = toByteArray(expected.get("requiredBytea"));
                byte[] actualRequired = rs.getBytes("requiredBytea");
                assertNotNull(actualRequired, "requiredBytea should not be null at index " + idx);
                assertArrayEquals(expectedRequired, actualRequired,
                        "requiredBytea mismatch at index " + idx);

                Object expectedOptionalObj = expected.get("optionalBytea");
                byte[] actualOptional = rs.getBytes("optionalBytea");
                if (expectedOptionalObj == null) {
                    assertNull(actualOptional, "optionalBytea should be null at index " + idx);
                } else {
                    assertNotNull(actualOptional, "optionalBytea should not be null at index " + idx);
                    assertArrayEquals(toByteArray(expectedOptionalObj), actualOptional,
                            "optionalBytea mismatch at index " + idx);
                }

                verifyByteaArray("requiredListWithNullableElements",
                        (List<Object>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyByteaArray("requiredListWithNonNullElements",
                        (List<Object>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyByteaArray("optionalList",
                        (List<Object>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyByteaArray("optionalListWithNonNullElements",
                        (List<Object>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                byte[] actualByteaAsString = rs.getBytes("byteaAsString");
                assertNotNull(actualByteaAsString, "byteaAsString should not be null at index " + idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyByteaArray(String fieldName, List<Object> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.BINARY, actualArray.getBaseType());
        byte[][] elements = (byte[][]) actualArray.getArray();
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
                assertArrayEquals(toByteArray(expectedObj), elements[i],
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }

    private byte[] toByteArray(Object value) {
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to byte[]");
    }
}
