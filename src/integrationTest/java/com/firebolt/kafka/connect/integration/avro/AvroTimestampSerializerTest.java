package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.utils.TestTag;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
public class AvroTimestampSerializerTest extends AvroBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("timestamp_test_table_avro");
    private String TOPIC_NAME = generateTopicName("timestamp-test-topic-avro");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-timestamp-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroTimestampSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro timestamp data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                timestampTableSchema(), avroTimestampSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroTimestampSchema().get());
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
        log.info("Running {} for Avro timestamp invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                timestampTableSchema(), avroTimestampSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroTimestampSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "2024-01-15 14:30:45");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "2025-12-31 23:59:59");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "abc");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "31-12-2025 23:59:59");

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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String timestampAsString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredTimestamp", toEpochMillis(LocalDateTime.of(2024, 1, 15, 14, 30, 45)));
        record.put("optionalTimestamp", toEpochMillis(LocalDateTime.of(2024, 2, 28, 16, 45, 30)));
        record.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 3, 1, 9, 0, 0)), null,
                toEpochMillis(LocalDateTime.of(2024, 3, 31, 17, 30, 15))));
        record.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 5, 1, 8, 30, 0)),
                toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30))));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("timestampAsString", timestampAsString);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        // Record 1: typical values
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredTimestamp", toEpochMillis(LocalDateTime.of(2024, 1, 15, 14, 30, 45)));
        r1.put("optionalTimestamp", toEpochMillis(LocalDateTime.of(2024, 2, 28, 16, 45, 30)));
        r1.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 3, 1, 9, 0, 0)), null,
                toEpochMillis(LocalDateTime.of(2024, 3, 31, 17, 30, 15)), null,
                toEpochMillis(LocalDateTime.of(2024, 4, 15, 12, 15, 45))));
        r1.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 5, 1, 8, 30, 0)),
                toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30)),
                toEpochMillis(LocalDateTime.of(2024, 7, 31, 19, 15, 0))));
        r1.put("optionalList", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 8, 1, 7, 0, 0)),
                toEpochMillis(LocalDateTime.of(2024, 9, 15, 14, 30, 45)),
                toEpochMillis(LocalDateTime.of(2024, 10, 31, 20, 45, 15))));
        r1.put("optionalListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 11, 1, 6, 15, 30)),
                toEpochMillis(LocalDateTime.of(2024, 12, 1, 21, 30, 45))));
        r1.put("timestampAsString", "2024-01-15 14:30:45");
        records.add(r1);

        // Record 2: Unix epoch, null optional, historical dates
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredTimestamp", toEpochMillis(LocalDateTime.of(1970, 1, 1, 0, 0, 0)));
        r2.put("optionalTimestamp", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(
                null, null,
                toEpochMillis(LocalDateTime.of(1970, 1, 1, 0, 0, 0)),
                toEpochMillis(LocalDateTime.of(2000, 1, 1, 12, 0, 0))));
        r2.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(1970, 1, 1, 0, 0, 0)),
                toEpochMillis(LocalDateTime.of(2000, 1, 1, 0, 0, 0)),
                toEpochMillis(LocalDateTime.of(2024, 6, 15, 14, 30, 45))));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("timestampAsString", "1970-01-01 00:00:00");
        records.add(r2);

        // Record 3: leap year dates, empty lists
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredTimestamp", toEpochMillis(LocalDateTime.of(2024, 2, 29, 12, 0, 0)));
        r3.put("optionalTimestamp", toEpochMillis(LocalDateTime.of(2020, 2, 29, 23, 59, 59)));
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("timestampAsString", "2024-02-29 12:00:00");
        records.add(r3);

        // Record 4: end-of-year timestamps, mixed arrays
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredTimestamp", toEpochMillis(LocalDateTime.of(2024, 12, 31, 23, 59, 59)));
        r4.put("optionalTimestamp", toEpochMillis(LocalDateTime.of(2025, 1, 1, 0, 0, 0)));
        r4.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 2, 29, 6, 30, 15)), null,
                toEpochMillis(LocalDateTime.of(2020, 2, 29, 18, 45, 30))));
        r4.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 2, 29, 9, 15, 45)),
                toEpochMillis(LocalDateTime.of(2020, 2, 29, 15, 30, 0)),
                toEpochMillis(LocalDateTime.of(2016, 2, 29, 21, 45, 15))));
        r4.put("optionalList", Arrays.asList(
                null, toEpochMillis(LocalDateTime.of(2024, 3, 15, 10, 30, 0)), null,
                toEpochMillis(LocalDateTime.of(2024, 9, 30, 16, 45, 30))));
        r4.put("optionalListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 4, 1, 8, 0, 0)),
                toEpochMillis(LocalDateTime.of(2024, 8, 31, 17, 30, 45))));
        r4.put("timestampAsString", "2024-12-31 23:59:59");
        records.add(r4);

        // Record 5: millisecond precision (timestamp-millis only supports ms)
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredTimestamp", toEpochMillis(LocalDateTime.of(2024, 1, 15, 14, 30, 45, 123_000_000)));
        r5.put("optionalTimestamp", toEpochMillis(LocalDateTime.of(2024, 6, 30, 9, 15, 30, 987_000_000)));
        r5.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 3, 1, 10, 0, 0, 500_000_000)),
                null,
                toEpochMillis(LocalDateTime.of(2024, 8, 15, 16, 45, 12, 123_000_000))));
        r5.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(LocalDateTime.of(2024, 5, 20, 8, 30, 45, 750_000_000)),
                toEpochMillis(LocalDateTime.of(2024, 9, 10, 20, 15, 30, 999_000_000))));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("timestampAsString", "2024-01-15 14:30:45.123");
        records.add(r5);

        return records;
    }

    private Supplier<String> timestampTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredTimestamp\" TIMESTAMP NOT NULL, " +
                "\"optionalTimestamp\" TIMESTAMP NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TIMESTAMP NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TIMESTAMP NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TIMESTAMP NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TIMESTAMP NOT NULL) NULL, " +
                "\"timestampAsString\" TIMESTAMP NOT NULL" +
                ")";
    }

    private Supplier<String> avroTimestampSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TimestampTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredTimestamp\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n" +
                "    {\"name\": \"optionalTimestamp\", \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}], \"default\": null},\n" +
                "    {\"name\": \"timestampAsString\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredTimestamp\", \"optionalTimestamp\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"timestampAsString\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                LocalDateTime expectedRequired = epochMillisToLocalDateTime((Long) expected.get("requiredTimestamp"));
                Timestamp actualRequired = rs.getTimestamp("requiredTimestamp");
                assertNotNull(actualRequired, "requiredTimestamp should not be null at index " + idx);
                assertEquals(expectedRequired, actualRequired.toLocalDateTime(),
                        "requiredTimestamp mismatch at index " + idx);

                Object expectedOptionalObj = expected.get("optionalTimestamp");
                Timestamp actualOptional = rs.getTimestamp("optionalTimestamp");
                if (expectedOptionalObj == null) {
                    assertNull(actualOptional, "optionalTimestamp should be null at index " + idx);
                } else {
                    assertNotNull(actualOptional, "optionalTimestamp should not be null at index " + idx);
                    LocalDateTime expectedOpt = epochMillisToLocalDateTime((Long) expectedOptionalObj);
                    assertEquals(expectedOpt, actualOptional.toLocalDateTime(),
                            "optionalTimestamp mismatch at index " + idx);
                }

                verifyTimestampArray("requiredListWithNullableElements",
                        (List<Object>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyTimestampArray("requiredListWithNonNullElements",
                        (List<Object>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyTimestampArray("optionalList",
                        (List<Object>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyTimestampArray("optionalListWithNonNullElements",
                        (List<Object>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                String expectedTsString = expected.get("timestampAsString").toString();
                Timestamp actualTsString = rs.getTimestamp("timestampAsString");
                assertNotNull(actualTsString, "timestampAsString should not be null at index " + idx);
                LocalDateTime expectedFromString = LocalDateTime.parse(expectedTsString.replace(' ', 'T'));
                assertEquals(expectedFromString, actualTsString.toLocalDateTime(),
                        "timestampAsString mismatch at index " + idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyTimestampArray(String fieldName, List<Object> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.TIMESTAMP, actualArray.getBaseType());
        Timestamp[] elements = (Timestamp[]) actualArray.getArray();
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
                LocalDateTime expectedTs = epochMillisToLocalDateTime((Long) expectedObj);
                assertEquals(expectedTs, elements[i].toLocalDateTime(),
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }

    private long toEpochMillis(LocalDateTime ldt) {
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private LocalDateTime epochMillisToLocalDateTime(long epochMillis) {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
