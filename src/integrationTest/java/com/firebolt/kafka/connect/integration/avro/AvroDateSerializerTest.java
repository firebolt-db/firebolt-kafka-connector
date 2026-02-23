package com.firebolt.kafka.connect.integration.avro;

import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
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
public class AvroDateSerializerTest extends AvroBaseIntegrationTest {

    private static final String TABLE_NAME = "date_test_table_avro";
    private static final String TOPIC_NAME = "date-test-topic-avro";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-date-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroDateSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro date data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                dateTableSchema(), avroDateSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroDateSchema().get());
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
        log.info("Running {} for Avro date invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                dateTableSchema(), avroDateSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroDateSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "2024-01-15");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "2025-12-31");
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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String dateAsString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredDate", toEpochDay(LocalDate.of(2024, 1, 15)));
        record.put("optionalDate", toEpochDay(LocalDate.of(2024, 2, 28)));
        record.put("requiredListWithNullableElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 3, 1)), null, toEpochDay(LocalDate.of(2024, 3, 31))));
        record.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 5, 1)), toEpochDay(LocalDate.of(2024, 6, 15))));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("dateAsString", dateAsString);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredDate", toEpochDay(LocalDate.of(2024, 1, 15)));
        r1.put("optionalDate", toEpochDay(LocalDate.of(2024, 2, 28)));
        r1.put("requiredListWithNullableElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 3, 1)), null, toEpochDay(LocalDate.of(2024, 3, 31)),
                null, toEpochDay(LocalDate.of(2024, 4, 15))));
        r1.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 5, 1)), toEpochDay(LocalDate.of(2024, 6, 15)),
                toEpochDay(LocalDate.of(2024, 7, 31))));
        r1.put("optionalList", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 8, 1)), toEpochDay(LocalDate.of(2024, 9, 15)),
                toEpochDay(LocalDate.of(2024, 10, 31))));
        r1.put("optionalListWithNonNullElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 11, 1)), toEpochDay(LocalDate.of(2024, 12, 1))));
        r1.put("dateAsString", "2024-01-15");
        records.add(r1);

        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredDate", toEpochDay(LocalDate.of(1970, 1, 1)));
        r2.put("optionalDate", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(
                null, null, toEpochDay(LocalDate.of(1970, 1, 1)),
                toEpochDay(LocalDate.of(2000, 1, 1))));
        r2.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochDay(LocalDate.of(1970, 1, 1)), toEpochDay(LocalDate.of(2000, 1, 1)),
                toEpochDay(LocalDate.of(2024, 6, 15))));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("dateAsString", "1970-01-01");
        records.add(r2);

        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredDate", toEpochDay(LocalDate.of(2024, 2, 29)));
        r3.put("optionalDate", toEpochDay(LocalDate.of(2020, 2, 29)));
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("dateAsString", "2024-02-29");
        records.add(r3);

        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredDate", toEpochDay(LocalDate.of(2024, 12, 31)));
        r4.put("optionalDate", toEpochDay(LocalDate.of(2025, 1, 1)));
        r4.put("requiredListWithNullableElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 2, 29)), null,
                toEpochDay(LocalDate.of(2020, 2, 29))));
        r4.put("requiredListWithNonNullElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 2, 29)),
                toEpochDay(LocalDate.of(2020, 2, 29)),
                toEpochDay(LocalDate.of(2016, 2, 29))));
        r4.put("optionalList", Arrays.asList(
                null, toEpochDay(LocalDate.of(2024, 3, 15)), null,
                toEpochDay(LocalDate.of(2024, 9, 30))));
        r4.put("optionalListWithNonNullElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 4, 1)),
                toEpochDay(LocalDate.of(2024, 8, 31))));
        r4.put("dateAsString", "2025-01-01");
        records.add(r4);

        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("recordId", 5);
        r5.put("requiredDate", toEpochDay(LocalDate.of(2000, 1, 1)));
        r5.put("optionalDate", toEpochDay(LocalDate.of(1999, 12, 31)));
        r5.put("requiredListWithNullableElements", Arrays.asList(
                toEpochDay(LocalDate.of(2024, 1, 1)), null));
        r5.put("requiredListWithNonNullElements", List.of(
                toEpochDay(LocalDate.of(2024, 6, 15))));
        r5.put("optionalList", null);
        r5.put("optionalListWithNonNullElements", null);
        r5.put("dateAsString", "2000-01-01");
        records.add(r5);

        return records;
    }

    private Supplier<String> dateTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredDate\" DATE NOT NULL, " +
                "\"optionalDate\" DATE NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(DATE NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(DATE NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(DATE NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(DATE NOT NULL) NULL, " +
                "\"dateAsString\" DATE NOT NULL" +
                ")";
    }

    private Supplier<String> avroDateSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"DateTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredDate\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n" +
                "    {\"name\": \"optionalDate\", \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"int\", \"logicalType\": \"date\"}}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": \"int\", \"logicalType\": \"date\"}}], \"default\": null},\n" +
                "    {\"name\": \"dateAsString\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredDate\", \"optionalDate\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"dateAsString\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                LocalDate expectedRequired = epochDayToLocalDate((Integer) expected.get("requiredDate"));
                Date actualRequired = rs.getDate("requiredDate");
                assertEquals(expectedRequired, actualRequired.toLocalDate(),
                        "requiredDate mismatch at index " + idx);

                Object expectedOptionalObj = expected.get("optionalDate");
                Date actualOptional = rs.getDate("optionalDate");
                if (expectedOptionalObj == null) {
                    assertNull(actualOptional, "optionalDate should be null at index " + idx);
                } else {
                    LocalDate expectedOpt = epochDayToLocalDate((Integer) expectedOptionalObj);
                    assertEquals(expectedOpt, actualOptional.toLocalDate(),
                            "optionalDate mismatch at index " + idx);
                }

                verifyDateArray("requiredListWithNullableElements",
                        (List<Object>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyDateArray("requiredListWithNonNullElements",
                        (List<Object>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyDateArray("optionalList",
                        (List<Object>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyDateArray("optionalListWithNonNullElements",
                        (List<Object>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                LocalDate expectedFromString = LocalDate.parse(expected.get("dateAsString").toString());
                Date actualFromString = rs.getDate("dateAsString");
                assertEquals(expectedFromString, actualFromString.toLocalDate(),
                        "dateAsString mismatch at index " + idx);

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyDateArray(String fieldName, List<Object> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.DATE, actualArray.getBaseType());
        Date[] elements = (Date[]) actualArray.getArray();
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
                LocalDate expectedDate = epochDayToLocalDate((Integer) expectedObj);
                assertEquals(expectedDate, elements[i].toLocalDate(),
                        fieldName + " element " + i + " mismatch at index " + idx);
            }
        }
    }

    private int toEpochDay(LocalDate date) {
        return (int) date.toEpochDay();
    }

    private LocalDate epochDayToLocalDate(int epochDay) {
        return LocalDate.ofEpochDay(epochDay);
    }
}
