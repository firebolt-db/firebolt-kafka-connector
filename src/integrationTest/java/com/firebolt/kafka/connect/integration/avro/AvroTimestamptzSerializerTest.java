package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.utils.TestTag;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
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
public class AvroTimestamptzSerializerTest extends AvroBaseIntegrationTest {

    private String TABLE_NAME = generateTableName("timestamptz_test_table_avro");
    private String TOPIC_NAME = generateTopicName("timestamptz-test-topic-avro");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-timestamptz-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroTimestamptzSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro timestamptz data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                timestamptzTableSchema(), avroTimestamptzSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroTimestamptzSchema().get());
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
        log.info("Running {} for Avro timestamptz invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                timestamptzTableSchema(), avroTimestamptzSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroTimestamptzSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "2024-01-15 14:30:45.123456+02");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "2025-12-31 23:59:59.000000+00");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "not-a-date");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "2024-02-30 00:00:00Z");

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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String timestamptzAsString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2))));
        record.put("optionalTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 2, 28, 16, 45, 30, 0, ZoneOffset.ofHours(2))));
        record.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 3, 1, 9, 0, 5, 0, ZoneOffset.ofHours(2))),
                null,
                toEpochMillis(OffsetDateTime.of(2024, 3, 31, 17, 30, 15, 0, ZoneOffset.ofHours(2)))));
        record.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 5, 1, 8, 30, 5, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, ZoneOffset.ofHours(2)))));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("timestamptzAsString", timestamptzAsString);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        // Record 1: typical values with UTC+2
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2))));
        r1.put("optionalTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 2, 28, 16, 45, 30, 0, ZoneOffset.ofHours(2))));
        r1.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 3, 1, 9, 0, 5, 0, ZoneOffset.ofHours(2))),
                null,
                toEpochMillis(OffsetDateTime.of(2024, 3, 31, 17, 30, 15, 0, ZoneOffset.ofHours(2))),
                null,
                toEpochMillis(OffsetDateTime.of(2024, 4, 15, 12, 15, 45, 0, ZoneOffset.ofHours(2)))));
        r1.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 5, 1, 8, 30, 5, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 7, 31, 19, 15, 9, 0, ZoneOffset.ofHours(2)))));
        r1.put("optionalList", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 8, 1, 7, 0, 5, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 9, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 10, 31, 20, 45, 15, 0, ZoneOffset.ofHours(2)))));
        r1.put("optionalListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 11, 1, 6, 15, 30, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 12, 1, 21, 30, 45, 0, ZoneOffset.ofHours(2)))));
        r1.put("timestamptzAsString", "2024-01-15 12:30:45.000000+00");
        records.add(r1);

        // Record 2: early post-epoch dates, null optional
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredTimestamptz", toEpochMillis(
                OffsetDateTime.of(1970, 1, 2, 14, 30, 1, 0, ZoneOffset.ofHours(2))));
        r2.put("optionalTimestamptz", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(
                null, null,
                toEpochMillis(OffsetDateTime.of(1970, 1, 2, 9, 0, 5, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2000, 1, 1, 3, 0, 30, 0, ZoneOffset.ofHours(2)))));
        r2.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(1970, 1, 1, 2, 0, 1, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2000, 1, 1, 2, 0, 1, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2)))));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("timestamptzAsString", "1970-01-02 12:30:01.000000+00");
        records.add(r2);

        // Record 3: leap year, empty lists
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 2, 29, 12, 0, 10, 0, ZoneOffset.ofHours(2))));
        r3.put("optionalTimestamptz", toEpochMillis(
                OffsetDateTime.of(2020, 2, 29, 23, 59, 59, 0, ZoneOffset.ofHours(2))));
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("timestamptzAsString", "2024-02-29 10:00:10.000000+00");
        records.add(r3);

        // Record 4: end-of-year, mixed arrays
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 0, ZoneOffset.ofHours(2))));
        r4.put("optionalTimestamptz", toEpochMillis(
                OffsetDateTime.of(2025, 1, 1, 0, 0, 2, 0, ZoneOffset.ofHours(2))));
        r4.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 2, 29, 6, 30, 15, 0, ZoneOffset.ofHours(2))),
                null,
                toEpochMillis(OffsetDateTime.of(2020, 2, 29, 18, 45, 30, 0, ZoneOffset.ofHours(2)))));
        r4.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 2, 29, 9, 15, 45, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2020, 2, 29, 15, 30, 10, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2016, 2, 29, 21, 45, 15, 0, ZoneOffset.ofHours(2)))));
        r4.put("optionalList", Arrays.asList(
                null,
                toEpochMillis(OffsetDateTime.of(2024, 3, 15, 10, 30, 10, 0, ZoneOffset.ofHours(2))),
                null,
                toEpochMillis(OffsetDateTime.of(2024, 9, 30, 16, 45, 30, 0, ZoneOffset.ofHours(2)))));
        r4.put("optionalListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 4, 1, 8, 0, 10, 0, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 8, 31, 17, 30, 45, 0, ZoneOffset.ofHours(2)))));
        r4.put("timestamptzAsString", "2024-12-31 21:59:59.000000+00");
        records.add(r4);

        // Record 5: millisecond precision (timestamp-millis only supports ms)
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 123_000_000, ZoneOffset.ofHours(2))));
        r5.put("optionalTimestamptz", toEpochMillis(
                OffsetDateTime.of(2024, 6, 30, 9, 15, 30, 987_000_000, ZoneOffset.ofHours(2))));
        r5.put("requiredListWithNullableElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 3, 1, 10, 0, 10, 500_000_000, ZoneOffset.ofHours(2))),
                null,
                toEpochMillis(OffsetDateTime.of(2024, 8, 15, 16, 45, 12, 123_000_000, ZoneOffset.ofHours(2)))));
        r5.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 5, 20, 8, 30, 45, 750_000_000, ZoneOffset.ofHours(2))),
                toEpochMillis(OffsetDateTime.of(2024, 9, 10, 20, 15, 30, 999_000_000, ZoneOffset.ofHours(2)))));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("timestamptzAsString", "2024-01-15 12:30:45.123000+00");
        records.add(r5);

        return records;
    }

    private Supplier<String> timestamptzTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredTimestamptz\" TIMESTAMPTZ NOT NULL, " +
                "\"optionalTimestamptz\" TIMESTAMPTZ NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TIMESTAMPTZ NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TIMESTAMPTZ NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TIMESTAMPTZ NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TIMESTAMPTZ NOT NULL) NULL, " +
                "\"timestamptzAsString\" TIMESTAMPTZ NOT NULL" +
                ")";
    }

    private Supplier<String> avroTimestamptzSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TimestamptzTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredTimestamptz\", \"type\": \"long\"},\n" +
                "    {\"name\": \"optionalTimestamptz\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"long\"]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": \"long\"}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"long\"]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"long\"}], \"default\": null},\n" +
                "    {\"name\": \"timestamptzAsString\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredTimestamptz\", \"optionalTimestamptz\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"timestamptzAsString\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                Instant expectedRequired = epochMillisToInstant((Long) expected.get("requiredTimestamptz"));
                Timestamp actualRequired = rs.getTimestamp("requiredTimestamptz");
                assertNotNull(actualRequired, "requiredTimestamptz should not be null at index " + idx);
                assertEquals(expectedRequired, actualRequired.toInstant(),
                        "requiredTimestamptz mismatch at index " + idx);

                Object expectedOptionalObj = expected.get("optionalTimestamptz");
                Timestamp actualOptional = rs.getTimestamp("optionalTimestamptz");
                if (expectedOptionalObj == null) {
                    assertNull(actualOptional, "optionalTimestamptz should be null at index " + idx);
                } else {
                    assertNotNull(actualOptional, "optionalTimestamptz should not be null at index " + idx);
                    Instant expectedOpt = epochMillisToInstant((Long) expectedOptionalObj);
                    assertEquals(expectedOpt, actualOptional.toInstant(),
                            "optionalTimestamptz mismatch at index " + idx);
                }

                verifyTimestamptzArray("requiredListWithNullableElements",
                        (List<Object>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyTimestamptzArray("requiredListWithNonNullElements",
                        (List<Object>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyTimestamptzArray("optionalList",
                        (List<Object>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyTimestamptzArray("optionalListWithNonNullElements",
                        (List<Object>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                String expectedTzString = expected.get("timestamptzAsString").toString();
                Timestamp actualTzString = rs.getTimestamp("timestamptzAsString");
                assertNotNull(actualTzString, "timestamptzAsString should not be null at index " + idx);
                OffsetDateTime expectedFromString = OffsetDateTime.parse(
                        expectedTzString.replace(' ', 'T'));
                assertEquals(expectedFromString.toInstant(), actualTzString.toInstant(),
                        "timestamptzAsString mismatch at index " + idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyTimestamptzArray(String fieldName, List<Object> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, actualArray.getBaseType());
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
                Instant expectedInstant = epochMillisToInstant((Long) expectedObj);
                assertEquals(expectedInstant, elements[i].toInstant(),
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }

    private long toEpochMillis(OffsetDateTime odt) {
        return odt.toInstant().toEpochMilli();
    }

    private Instant epochMillisToInstant(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis);
    }
}
