package com.firebolt.kafka.connect.integration.avro;

import com.firebolt.kafka.connect.utils.TestTag;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
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
public class AvroNumericSerializerTest extends AvroBaseIntegrationTest {

    private static final String TABLE_NAME = "numeric_test_table_avro";
    private static final String TOPIC_NAME = "numeric-test-topic-avro";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";
    private static final int SCALE = 9;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("avro-numeric-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAvroNumericSerialization(Map<String, String> connectorOverride, String description) throws Exception {
        log.info("Running {} for Avro numeric data type", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                numericTableSchema(), avroNumericSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroNumericSchema().get());
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
        log.info("Running {} for Avro numeric invalid-value resilience", description);

        setupAvroTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                numericTableSchema(), avroNumericSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(avroNumericSchema().get());

        GenericData.Record valid1 = createValidRecord(avroSchema, 201, "42.42");
        GenericData.Record valid2 = createValidRecord(avroSchema, 202, "-17.5");
        GenericData.Record invalid1 = createValidRecord(avroSchema, 203, "abc");
        GenericData.Record invalid2 = createValidRecord(avroSchema, 204, "1,23");

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

    private GenericData.Record createValidRecord(Schema schema, int recordId, String bigDecimalAsString) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("recordId", recordId);
        record.put("requiredNumeric", decimalToBytes(new BigDecimal("42.123456789")));
        record.put("optionalNumeric", decimalToBytes(new BigDecimal("100.987654321")));
        record.put("requiredListWithNullableElements", Arrays.asList(
                decimalToBytes(new BigDecimal("1.111111111")), null, decimalToBytes(new BigDecimal("3.333333333"))));
        record.put("requiredListWithNonNullElements", Arrays.asList(
                decimalToBytes(new BigDecimal("10.123456789")), decimalToBytes(new BigDecimal("20.234567890"))));
        record.put("optionalList", null);
        record.put("optionalListWithNonNullElements", null);
        record.put("bigDecimalFromString", bigDecimalAsString);
        record.put("optionalInt", recordId * 10);
        record.put("optionalLong", (long) recordId * 100);
        return record;
    }

    private List<GenericData.Record> createTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("recordId", 1);
        r1.put("requiredNumeric", decimalToBytes(new BigDecimal("42.123456789")));
        r1.put("optionalNumeric", decimalToBytes(new BigDecimal("100.987654321")));
        r1.put("requiredListWithNullableElements", Arrays.asList(
                decimalToBytes(new BigDecimal("1.111111111")),
                decimalToBytes(new BigDecimal("2.222222222")),
                decimalToBytes(new BigDecimal("3.333333333"))));
        r1.put("requiredListWithNonNullElements", Arrays.asList(
                decimalToBytes(new BigDecimal("10.123456789")),
                decimalToBytes(new BigDecimal("20.234567890")),
                decimalToBytes(new BigDecimal("30.345678901"))));
        r1.put("optionalList", Arrays.asList(
                decimalToBytes(new BigDecimal("100.111111111")),
                decimalToBytes(new BigDecimal("200.222222222")),
                decimalToBytes(new BigDecimal("300.333333333"))));
        r1.put("optionalListWithNonNullElements", Arrays.asList(
                decimalToBytes(new BigDecimal("111.444444444")),
                decimalToBytes(new BigDecimal("222.555555555")),
                decimalToBytes(new BigDecimal("333.666666666"))));
        r1.put("bigDecimalFromString", "1.5");
        r1.put("optionalInt", 100);
        r1.put("optionalLong", 123456789012345L);
        records.add(r1);

        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("recordId", 2);
        r2.put("requiredNumeric", decimalToBytes(new BigDecimal("99999999999999999999999999999.123456789")));
        r2.put("optionalNumeric", null);
        r2.put("requiredListWithNullableElements", Arrays.asList(
                null,
                null,
                decimalToBytes(new BigDecimal("99999999999999999999999999999.123456789")),
                decimalToBytes(new BigDecimal("-99999999999999999999999999999.987654321"))));
        r2.put("requiredListWithNonNullElements", Arrays.asList(
                decimalToBytes(new BigDecimal("-99999999999999999999999999999.987654321")),
                decimalToBytes(new BigDecimal("123.456789012")),
                decimalToBytes(new BigDecimal("-123.456789012")),
                decimalToBytes(new BigDecimal("99999999999999999999999999999.123456789"))));
        r2.put("optionalList", null);
        r2.put("optionalListWithNonNullElements", null);
        r2.put("bigDecimalFromString", String.valueOf(Long.MIN_VALUE));
        r2.put("optionalInt", null);
        r2.put("optionalLong", null);
        records.add(r2);

        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("recordId", 3);
        r3.put("requiredNumeric", decimalToBytes(new BigDecimal("0.000000001")));
        r3.put("optionalNumeric", decimalToBytes(new BigDecimal("-0.000000001")));
        r3.put("requiredListWithNullableElements", new ArrayList<>());
        r3.put("requiredListWithNonNullElements", new ArrayList<>());
        r3.put("optionalList", new ArrayList<>());
        r3.put("optionalListWithNonNullElements", new ArrayList<>());
        r3.put("bigDecimalFromString", "0.000000001");
        r3.put("optionalInt", Integer.MAX_VALUE);
        r3.put("optionalLong", Long.MAX_VALUE);
        records.add(r3);

        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("recordId", 4);
        r4.put("requiredNumeric", decimalToBytes(BigDecimal.ZERO));
        r4.put("optionalNumeric", decimalToBytes(BigDecimal.ZERO));
        r4.put("requiredListWithNullableElements", Arrays.asList(
                decimalToBytes(new BigDecimal("1.111111111")), null, decimalToBytes(new BigDecimal("2.222222222"))));
        r4.put("requiredListWithNonNullElements", Arrays.asList(
                decimalToBytes(new BigDecimal("10.123456789")), decimalToBytes(new BigDecimal("20.234567890"))));
        r4.put("optionalList", Arrays.asList(
                decimalToBytes(new BigDecimal("-100.555555555")), decimalToBytes(new BigDecimal("200.666666666")), null));
        r4.put("optionalListWithNonNullElements", Arrays.asList(
                decimalToBytes(new BigDecimal("-100.777777777")), decimalToBytes(BigDecimal.ZERO), decimalToBytes(new BigDecimal("200.888888888"))));
        r4.put("bigDecimalFromString", "0");
        r4.put("optionalInt", Integer.MIN_VALUE);
        r4.put("optionalLong", Long.MIN_VALUE);
        records.add(r4);

        return records;
    }

    private Supplier<String> numericTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredNumeric\" NUMERIC(38,9) NOT NULL, " +
                "\"optionalNumeric\" NUMERIC(38,9) NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(NUMERIC(38,9) NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(NUMERIC(38,9) NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(NUMERIC(38,9) NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(NUMERIC(38,9) NOT NULL) NULL, " +
                "\"bigDecimalFromString\" NUMERIC(38,9) NOT NULL, " +
                "\"optionalInt\" NUMERIC(38,9) NULL, " +
                "\"optionalLong\" NUMERIC(38,9) NULL" +
                ")";
    }

    private Supplier<String> avroNumericSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"NumericTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"recordId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"requiredNumeric\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}},\n" +
                "    {\"name\": \"optionalNumeric\", \"type\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}], \"default\": null},\n" +
                "    {\"name\": \"requiredListWithNullableElements\", \"type\": {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}]}},\n" +
                "    {\"name\": \"requiredListWithNonNullElements\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}}},\n" +
                "    {\"name\": \"optionalList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}]}], \"default\": null},\n" +
                "    {\"name\": \"optionalListWithNonNullElements\", \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}}], \"default\": null},\n" +
                "    {\"name\": \"bigDecimalFromString\", \"type\": \"string\"},\n" +
                "    {\"name\": \"optionalInt\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"optionalLong\", \"type\": [\"null\", \"long\"], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount);

        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredNumeric\", \"optionalNumeric\", " +
                "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
                "\"optionalList\", \"optionalListWithNonNullElements\", \"bigDecimalFromString\", \"optionalInt\", \"optionalLong\" " +
                "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expectedRecords.size());
                GenericData.Record expected = expectedRecords.get(idx);

                assertEquals(expected.get("recordId"), rs.getInt("recordId"));

                BigDecimal expectedRequired = bytesToDecimal((ByteBuffer) expected.get("requiredNumeric"));
                BigDecimal actualRequired = rs.getBigDecimal("requiredNumeric");
                assertEquals(0, expectedRequired.compareTo(actualRequired),
                        "requiredNumeric mismatch at index " + idx +
                        " (expected: " + expectedRequired + ", actual: " + actualRequired + ")");

                Object expectedOptionalObj = expected.get("optionalNumeric");
                BigDecimal actualOptional = rs.getBigDecimal("optionalNumeric");
                if (expectedOptionalObj == null) {
                    assertNull(actualOptional, "optionalNumeric should be null at index " + idx);
                } else {
                    BigDecimal expectedOptional = bytesToDecimal((ByteBuffer) expectedOptionalObj);
                    assertEquals(0, expectedOptional.compareTo(actualOptional),
                            "optionalNumeric mismatch at index " + idx +
                            " (expected: " + expectedOptional + ", actual: " + actualOptional + ")");
                }

                verifyNumericArray("requiredListWithNullableElements",
                        (List<Object>) expected.get("requiredListWithNullableElements"),
                        rs.getArray("requiredListWithNullableElements"), idx);
                verifyNumericArray("requiredListWithNonNullElements",
                        (List<Object>) expected.get("requiredListWithNonNullElements"),
                        rs.getArray("requiredListWithNonNullElements"), idx);
                verifyNumericArray("optionalList",
                        (List<Object>) expected.get("optionalList"),
                        rs.getArray("optionalList"), idx);
                verifyNumericArray("optionalListWithNonNullElements",
                        (List<Object>) expected.get("optionalListWithNonNullElements"),
                        rs.getArray("optionalListWithNonNullElements"), idx);

                BigDecimal expectedFromString = new BigDecimal(expected.get("bigDecimalFromString").toString().trim());
                BigDecimal actualFromString = rs.getBigDecimal("bigDecimalFromString");
                assertEquals(0, expectedFromString.compareTo(actualFromString),
                        "bigDecimalFromString mismatch at index " + idx);

                Object expectedOptionalInt = expected.get("optionalInt");
                BigDecimal actualOptionalInt = rs.getBigDecimal("optionalInt");
                if (expectedOptionalInt == null) {
                    assertNull(actualOptionalInt, "optionalInt should be null at index " + idx);
                } else {
                    assertEquals(0, new BigDecimal((Integer) expectedOptionalInt).compareTo(actualOptionalInt),
                            "optionalInt mismatch at index " + idx);
                }

                Object expectedOptionalLong = expected.get("optionalLong");
                BigDecimal actualOptionalLong = rs.getBigDecimal("optionalLong");
                if (expectedOptionalLong == null) {
                    assertNull(actualOptionalLong, "optionalLong should be null at index " + idx);
                } else {
                    assertEquals(0, BigDecimal.valueOf((Long) expectedOptionalLong).compareTo(actualOptionalLong),
                            "optionalLong mismatch at index " + idx);
                }

                idx++;
            }
            assertEquals(expectedRecords.size(), idx);
        }
    }

    private void verifyNumericArray(String fieldName, List<Object> expected, Array actualArray, int idx) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArray, fieldName + " should not be null at index " + idx);
        assertEquals(Types.NUMERIC, actualArray.getBaseType());
        BigDecimal[] elements = (BigDecimal[]) actualArray.getArray();
        assertEquals(expected.size(), elements.length,
                fieldName + " size mismatch at index " + idx);
        for (int i = 0; i < expected.size(); i++) {
            Object expectedObj = expected.get(i);
            if (expectedObj == null) {
                assertNull(elements[i],
                        fieldName + " element " + i + " should be null at index " + idx);
            } else {
                BigDecimal expectedDecimal = bytesToDecimal((ByteBuffer) expectedObj);
                assertNotNull(elements[i],
                        fieldName + " element " + i + " should not be null at index " + idx);
                assertEquals(0, expectedDecimal.compareTo(elements[i]),
                        fieldName + " element " + i + " mismatch at index " + idx +
                        " (expected: " + expectedDecimal + ", actual: " + elements[i] + ")");
            }
        }
    }

    private ByteBuffer decimalToBytes(BigDecimal value) {
        BigDecimal scaled = value.setScale(SCALE, RoundingMode.HALF_UP);
        return ByteBuffer.wrap(scaled.unscaledValue().toByteArray());
    }

    private BigDecimal bytesToDecimal(ByteBuffer buffer) {
        return new BigDecimal(new BigInteger(buffer.array()), SCALE);
    }
}
