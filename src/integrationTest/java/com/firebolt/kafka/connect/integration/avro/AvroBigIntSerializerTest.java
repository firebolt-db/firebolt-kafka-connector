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
public class AvroBigIntSerializerTest extends AvroBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("bigint_test_table_avro");
    private String TOPIC_NAME = generateTopicName("bigint-test-topic-avro");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-bigint-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroBigIntSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro bigint data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                bigintTableSchema(), avroBigIntSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroBigIntSchema().get());
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
        log.info("Running {} for Avro bigint invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                bigintTableSchema(), avroBigIntSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroBigIntSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "1234567890123");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "-999999999999");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "abc");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "9223372036854775808");

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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String bigIntAsString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredBigInt", 42L);
        record.put("optionalBigInt", 123L);
        record.put("requiredListWithNullableElements", Arrays.asList(1L, null, 3L));
        record.put("requiredListWithNonNullElements", Arrays.asList(10L, 20L, 30L));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("bigIntAsString", bigIntAsString);
        record.put("optionalInt", recordId * 10);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredBigInt", 42L);
        r1.put("optionalBigInt", 123L);
        r1.put("requiredListWithNullableElements", Arrays.asList(1L, 2L, 3L));
        r1.put("requiredListWithNonNullElements", Arrays.asList(10L, 20L, 30L));
        r1.put("optionalList", Arrays.asList(100L, 200L, 300L));
        r1.put("optionalListWithNonNullElements", Arrays.asList(1000L, 2000L, 3000L));
        r1.put("bigIntAsString", "1");
        r1.put("optionalInt", 100);
        records.add(r1);

        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredBigInt", Long.MIN_VALUE);
        r2.put("optionalBigInt", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(null, null, Long.MAX_VALUE, Long.MIN_VALUE));
        r2.put("requiredListWithNonNullElements", Arrays.asList(Long.MIN_VALUE, 123L, -123L, Long.MAX_VALUE));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("bigIntAsString", String.valueOf(Long.MIN_VALUE));
        r2.put("optionalInt", null);
        records.add(r2);

        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredBigInt", Long.MAX_VALUE);
        r3.put("optionalBigInt", -999L);
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("bigIntAsString", String.valueOf(Long.MAX_VALUE));
        r3.put("optionalInt", Integer.MAX_VALUE);
        records.add(r3);

        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredBigInt", 0L);
        r4.put("optionalBigInt", 9223372036854775L);
        r4.put("requiredListWithNullableElements", Arrays.asList(1L, null, 2L));
        r4.put("requiredListWithNonNullElements", Arrays.asList(10L, 20L));
        r4.put("optionalList", Arrays.asList(-100L, 200L, null));
        r4.put("optionalListWithNonNullElements", Arrays.asList(-100L, 0L, 200L));
        r4.put("bigIntAsString", "0");
        r4.put("optionalInt", Integer.MIN_VALUE);
        records.add(r4);

        return records;
    }

    private Supplier<String> bigintTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredBigInt\" BIGINT NOT NULL, " +
                "\"optionalBigInt\" BIGINT NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(BIGINT NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(BIGINT NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(BIGINT NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(BIGINT NOT NULL) NULL, " +
                "\"bigIntAsString\" BIGINT NOT NULL, " +
                "\"optionalInt\" BIGINT NULL" +
                ")";
    }

    private Supplier<String> avroBigIntSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"BigIntTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredBigInt\", \"type\": \"long\"},\n" +
                "    {\"name\": \"optionalBigInt\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"long\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"long\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"long\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"long\"}], \"default\": null},\n" +
                "    {\"name\": \"bigIntAsString\", \"type\": \"string\"},\n" +
                "    {\"name\": \"optionalInt\", \"type\": [\"null\", \"int\"], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredBigInt\", \"optionalBigInt\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"bigIntAsString\", \"optionalInt\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));
                assertEquals(expected.get("requiredBigInt"), rs.getLong("requiredBigInt"));

                Object expectedOptional = expected.get("optionalBigInt");
                Long actualOptional = rs.getObject("optionalBigInt", Long.class);
                if (expectedOptional == null) {
                    assertNull(actualOptional);
                } else {
                    assertEquals(expectedOptional, actualOptional);
                }

                verifyLongArray("requiredListWithNullableElements",
                        (List<Long>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyLongArray("requiredListWithNonNullElements",
                        (List<Long>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyLongArray("optionalList",
                        (List<Long>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyLongArray("optionalListWithNonNullElements",
                        (List<Long>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                long expectedBigIntAsString = Long.parseLong(expected.get("bigIntAsString").toString());
                assertEquals(expectedBigIntAsString, rs.getLong("bigIntAsString"),
                        "bigIntAsString mismatch at index " + idx);

                Object expectedOptionalInt = expected.get("optionalInt");
                Long actualOptionalInt = rs.getObject("optionalInt", Long.class);
                if (expectedOptionalInt == null) {
                    assertNull(actualOptionalInt, "optionalInt should be null at index " + idx);
                } else {
                    assertEquals(((Integer) expectedOptionalInt).longValue(), actualOptionalInt,
                            "optionalInt mismatch at index " + idx);
                }

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyLongArray(String fieldName, List<Long> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.BIGINT, actualArray.getBaseType());
        Long[] elements = (Long[]) actualArray.getArray();
        assertEquals(expected, Arrays.asList(elements), fieldName + " mismatch at index " + idx);
    }
}
