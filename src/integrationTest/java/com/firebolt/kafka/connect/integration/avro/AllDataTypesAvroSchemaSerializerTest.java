package com.firebolt.kafka.connect.integration.avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class AllDataTypesAvroSchemaSerializerTest extends AvroBaseIntegrationTest {

    private static final String ALL_DATA_TYPES_TABLE_NAME = "all_data_types_test_table_avro";
    private static final String ALL_DATA_TYPES_TOPIC_NAME = "all-data-types-test-topic-avro";
    private static final String ALL_DATA_TYPES_SCHEMA_SUBJECT = ALL_DATA_TYPES_TOPIC_NAME + "-value";
    private static final int DECIMAL_SCALE = 9;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("all-data-types-avro-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupAvroTestResources(ALL_DATA_TYPES_TABLE_NAME, ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAllDataTypesAvroSchemaSerializationAndKafkaConnectProcessing(Map<String, String> connectorOverride, String testDescription) throws Exception {
        log.info("Running {} for all data types (Avro)", testDescription);

        setupAvroTestResources(ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_TABLE_NAME, ALL_DATA_TYPES_SCHEMA_SUBJECT,
                allDataTypesTableSchema(), allDataTypesAvroSchema(), connectorOverride);

        Schema avroSchema = new Schema.Parser().parse(allDataTypesAvroSchema().get());
        List<GenericData.Record> testRecords = generateAllDataTypesTestRecords(avroSchema);

        try (Producer<String, Object> producer = initializeAvroProducer()) {
            for (int i = 0; i < testRecords.size(); i++) {
                producer.send(new ProducerRecord<>(ALL_DATA_TYPES_TOPIC_NAME, String.valueOf(i + 1), testRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(ALL_DATA_TYPES_TABLE_NAME, testRecords.size());
        verifyAllDataTypesRecordsInFirebolt(testRecords);
    }

    private Supplier<String> allDataTypesTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"colInteger\" INTEGER NOT NULL, " +
                "\"colBigint\" BIGINT, " +
                "\"colNumeric\" NUMERIC(38,9), " +
                "\"colReal\" REAL, " +
                "\"colDoublePrecision\" DOUBLE PRECISION, " +
                "\"colBoolean\" BOOLEAN, " +
                "\"colText\" TEXT, " +
                "\"colDate\" DATE, " +
                "\"colTimestamp\" TIMESTAMP, " +
                "\"colTimestamptz\" TIMESTAMPTZ, " +
                "\"colBytea\" BYTEA, " +
                "\"colArrayTextNullable\" ARRAY(TEXT NULL), " +
                "\"colArrayTextNotNull\" ARRAY(TEXT NOT NULL), " +
                "\"colArrayIntSyntax1\" ARRAY(INTEGER), " +
                "\"colArrayIntSyntax2\" INTEGER[], " +
                "\"colArrayDate\" ARRAY(DATE), " +
                "\"colArrayReal\" ARRAY(REAL), " +
                "\"colArrayNumeric\" ARRAY(NUMERIC), " +
                "\"colArrayDoublePrecision\" ARRAY(DOUBLE PRECISION), " +
                "\"colArrayTimestamptz\" ARRAY(TIMESTAMPTZ), " +
                "\"colArrayTimestamp\" ARRAY(TIMESTAMP) " +
                ");";
    }

    private Supplier<String> allDataTypesAvroSchema() {
        return () -> "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"AllDataTypesTestRecord\",\n" +
                "  \"namespace\": \"com.firebolt.kafka.connect.integration.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"colInteger\", \"type\": \"int\"},\n" +
                "    {\"name\": \"colBigint\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
                "    {\"name\": \"colNumeric\", \"type\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}], \"default\": null},\n" +
                "    {\"name\": \"colReal\", \"type\": [\"null\", \"float\"], \"default\": null},\n" +
                "    {\"name\": \"colDoublePrecision\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
                "    {\"name\": \"colBoolean\", \"type\": [\"null\", \"boolean\"], \"default\": null},\n" +
                "    {\"name\": \"colText\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"colDate\", \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}], \"default\": null},\n" +
                "    {\"name\": \"colTimestamp\", \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}], \"default\": null},\n" +
                "    {\"name\": \"colTimestamptz\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
                "    {\"name\": \"colBytea\", \"type\": [\"null\", \"bytes\"], \"default\": null},\n" +
                "    {\"name\": \"colArrayTextNullable\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"string\"]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayTextNotNull\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null},\n" +
                "    {\"name\": \"colArrayIntSyntax1\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"int\"]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayIntSyntax2\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"int\"]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayDate\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayReal\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"float\"]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayNumeric\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 9}]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayDoublePrecision\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"double\"]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayTimestamptz\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", \"long\"]}], \"default\": null},\n" +
                "    {\"name\": \"colArrayTimestamp\", \"type\": [\"null\", {\"type\": \"array\", \"items\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}]}], \"default\": null}\n" +
                "  ]\n" +
                "}";
    }

    private List<GenericData.Record> generateAllDataTypesTestRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        // Record 1: typical values
        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("colInteger", 1);
        r1.put("colBigint", 1000L);
        r1.put("colNumeric", decimalToBytes(new BigDecimal("12345678901234567890123456789.123456789")));
        r1.put("colReal", 1.5f);
        r1.put("colDoublePrecision", 1.23456789);
        r1.put("colBoolean", true);
        r1.put("colText", "Basic Test Data");
        r1.put("colDate", toEpochDay(LocalDate.of(2024, 1, 1)));
        r1.put("colTimestamp", toEpochMicros(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0)));
        r1.put("colTimestamptz", toEpochMillis(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)));
        r1.put("colBytea", ByteBuffer.wrap("hello".getBytes()));
        r1.put("colArrayTextNullable", Arrays.asList("apple", null, "banana", "cherry"));
        r1.put("colArrayTextNotNull", Arrays.asList("apple", "banana", "cherry", "date"));
        r1.put("colArrayIntSyntax1", Arrays.asList(1, 2, 3, 4, 5));
        r1.put("colArrayIntSyntax2", Arrays.asList(10, 20, 30, 40, 50));
        r1.put("colArrayDate", Arrays.asList(toEpochDay(LocalDate.of(2024, 1, 1)), toEpochDay(LocalDate.of(2024, 1, 2)), toEpochDay(LocalDate.of(2024, 1, 3))));
        r1.put("colArrayReal", Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f));
        r1.put("colArrayNumeric", Arrays.asList(
                decimalToBytes(new BigDecimal("100.123456789")),
                decimalToBytes(new BigDecimal("200.987654321")),
                decimalToBytes(new BigDecimal("300.555555555"))));
        r1.put("colArrayDoublePrecision", Arrays.asList(1.11111, 2.22222, 3.33333, 4.44444));
        r1.put("colArrayTimestamptz", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 1, 1, 12, 0, 10, 0, ZoneOffset.UTC)),
                toEpochMillis(OffsetDateTime.of(2024, 1, 2, 13, 30, 10, 0, ZoneOffset.UTC)),
                toEpochMillis(OffsetDateTime.of(2024, 1, 3, 15, 45, 30, 0, ZoneOffset.UTC))));
        r1.put("colArrayTimestamp", Arrays.asList(
                toEpochMicros(LocalDateTime.of(2024, 1, 1, 12, 0, 10, 0)),
                toEpochMicros(LocalDateTime.of(2024, 1, 2, 13, 30, 10, 0)),
                toEpochMicros(LocalDateTime.of(2024, 1, 3, 15, 45, 30, 0))));
        records.add(r1);

        // Record 2: edge case values
        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("colInteger", 2);
        r2.put("colBigint", Long.MAX_VALUE);
        r2.put("colNumeric", decimalToBytes(new BigDecimal("99999999999999999999999999999.999999999")));
        r2.put("colReal", Float.MAX_VALUE);
        r2.put("colDoublePrecision", Double.MAX_VALUE);
        r2.put("colBoolean", false);
        r2.put("colText", "Edge Case Test Data with very long text that might exceed normal limits");
        r2.put("colDate", toEpochDay(LocalDate.of(2099, 12, 31)));
        r2.put("colTimestamp", toEpochMicros(LocalDateTime.of(2099, 12, 31, 23, 59, 59, 999999000)));
        r2.put("colTimestamptz", toEpochMillis(OffsetDateTime.of(2099, 12, 31, 23, 59, 59, 999999000, ZoneOffset.UTC)));
        r2.put("colBytea", ByteBuffer.wrap("edge_case_binary_data".getBytes()));
        r2.put("colArrayTextNullable", null);
        r2.put("colArrayTextNotNull", null);
        r2.put("colArrayIntSyntax1", null);
        r2.put("colArrayIntSyntax2", null);
        r2.put("colArrayDate", null);
        r2.put("colArrayReal", null);
        r2.put("colArrayNumeric", null);
        r2.put("colArrayDoublePrecision", null);
        r2.put("colArrayTimestamptz", null);
        r2.put("colArrayTimestamp", null);
        records.add(r2);

        // Record 3: nullable values
        GenericData.Record r3 = new GenericData.Record(schema);
        r3.put("colInteger", 3);
        r3.put("colBigint", null);
        r3.put("colNumeric", null);
        r3.put("colReal", null);
        r3.put("colDoublePrecision", null);
        r3.put("colBoolean", null);
        r3.put("colText", null);
        r3.put("colDate", null);
        r3.put("colTimestamp", null);
        r3.put("colTimestamptz", null);
        r3.put("colBytea", null);
        r3.put("colArrayTextNullable", null);
        r3.put("colArrayTextNotNull", null);
        r3.put("colArrayIntSyntax1", null);
        r3.put("colArrayIntSyntax2", null);
        r3.put("colArrayDate", null);
        r3.put("colArrayReal", null);
        r3.put("colArrayNumeric", null);
        r3.put("colArrayDoublePrecision", null);
        r3.put("colArrayTimestamptz", null);
        r3.put("colArrayTimestamp", null);
        records.add(r3);

        // Record 4: geographic sample data
        GenericData.Record r4 = new GenericData.Record(schema);
        r4.put("colInteger", 4);
        r4.put("colBigint", 1000L);
        r4.put("colNumeric", decimalToBytes(new BigDecimal("12345678901234567890123456789.123456789")));
        r4.put("colReal", 1.5f);
        r4.put("colDoublePrecision", 1.23456789);
        r4.put("colBoolean", true);
        r4.put("colText", "San Francisco");
        r4.put("colDate", toEpochDay(LocalDate.of(2024, 1, 1)));
        r4.put("colTimestamp", toEpochMicros(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0)));
        r4.put("colTimestamptz", toEpochMillis(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)));
        r4.put("colBytea", ByteBuffer.wrap("hello".getBytes()));
        r4.put("colArrayTextNullable", Arrays.asList("San Francisco", "New York", null, "London", "Tokyo"));
        r4.put("colArrayTextNotNull", Arrays.asList("California", "New York", "England", "Japan"));
        r4.put("colArrayIntSyntax1", Arrays.asList(37, 40, 51, 35));
        r4.put("colArrayIntSyntax2", Arrays.asList(774, 840, 130, 392));
        r4.put("colArrayDate", Arrays.asList(toEpochDay(LocalDate.of(2024, 1, 1)), toEpochDay(LocalDate.of(2024, 1, 2)), toEpochDay(LocalDate.of(2024, 1, 3))));
        r4.put("colArrayReal", Arrays.asList(37.7749f, 40.7128f, 51.5074f, 35.6762f));
        r4.put("colArrayNumeric", null);
        r4.put("colArrayDoublePrecision", null);
        r4.put("colArrayTimestamptz", null);
        r4.put("colArrayTimestamp", null);
        records.add(r4);

        // Record 5: variety of data patterns
        GenericData.Record r5 = new GenericData.Record(schema);
        r5.put("colInteger", 5);
        r5.put("colBigint", -1000L);
        r5.put("colNumeric", decimalToBytes(new BigDecimal("-12345678901234567890123456789.123456789")));
        r5.put("colReal", -1.5f);
        r5.put("colDoublePrecision", -1.23456789);
        r5.put("colBoolean", true);
        r5.put("colText", "Variety Test Data with special characters: !@#$%^&*()");
        r5.put("colDate", toEpochDay(LocalDate.of(1970, 1, 1)));
        r5.put("colTimestamp", toEpochMicros(LocalDateTime.of(2000, 1, 1, 0, 0, 30, 0)));
        r5.put("colTimestamptz", toEpochMillis(OffsetDateTime.of(2000, 1, 1, 0, 0, 35, 0, ZoneOffset.UTC)));
        r5.put("colBytea", ByteBuffer.wrap("variety_binary_data".getBytes()));
        r5.put("colArrayTextNullable", Arrays.asList("apple", null, "banana"));
        r5.put("colArrayTextNotNull", Arrays.asList("apple", "banana", "cherry"));
        r5.put("colArrayIntSyntax1", Arrays.asList(1, 2, 3));
        r5.put("colArrayIntSyntax2", Arrays.asList(10, 20, 30));
        r5.put("colArrayDate", Arrays.asList(toEpochDay(LocalDate.of(2024, 1, 1)), toEpochDay(LocalDate.of(2024, 1, 2)), toEpochDay(LocalDate.of(2024, 1, 3))));
        r5.put("colArrayReal", Arrays.asList(1.1f, 2.2f, 3.3f));
        r5.put("colArrayNumeric", Arrays.asList(
                decimalToBytes(new BigDecimal("100.123456789")),
                decimalToBytes(new BigDecimal("200.987654321")),
                decimalToBytes(new BigDecimal("300.555555555"))));
        r5.put("colArrayDoublePrecision", Arrays.asList(1.11111, 2.22222, 3.33333, 4.44444));
        r5.put("colArrayTimestamptz", Arrays.asList(
                toEpochMillis(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)),
                toEpochMillis(OffsetDateTime.of(2024, 1, 2, 13, 30, 20, 0, ZoneOffset.UTC)),
                toEpochMillis(OffsetDateTime.of(2024, 1, 3, 15, 45, 30, 0, ZoneOffset.UTC))));
        r5.put("colArrayTimestamp", Arrays.asList(
                toEpochMicros(LocalDateTime.of(2024, 1, 1, 12, 0, 25, 0)),
                toEpochMicros(LocalDateTime.of(2024, 1, 2, 13, 30, 25, 0)),
                toEpochMicros(LocalDateTime.of(2024, 1, 3, 15, 45, 30, 0))));
        records.add(r5);

        return records;
    }

    private int toEpochDay(LocalDate date) {
        return (int) date.toEpochDay();
    }

    private long toEpochMicros(LocalDateTime ldt) {
        return ldt.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000L + ldt.getNano() / 1_000L;
    }

    private long toEpochMillis(OffsetDateTime odt) {
        return odt.toInstant().toEpochMilli();
    }

    private ByteBuffer decimalToBytes(BigDecimal value) {
        BigDecimal scaled = value.setScale(DECIMAL_SCALE, RoundingMode.HALF_UP);
        return ByteBuffer.wrap(scaled.unscaledValue().toByteArray());
    }

    private BigDecimal bytesToDecimal(ByteBuffer buffer) {
        return new BigDecimal(new BigInteger(buffer.array()), DECIMAL_SCALE);
    }

    @SuppressWarnings("unchecked")
    private void verifyAllDataTypesRecordsInFirebolt(List<GenericData.Record> expectedRecords) throws SQLException {
        String selectQuery = "SELECT \"colInteger\", \"colBigint\", \"colNumeric\", \"colReal\", \"colDoublePrecision\", \"colBoolean\", \"colText\", \"colDate\", " +
                "\"colTimestamp\", \"colTimestamptz\", \"colBytea\", \"colArrayTextNullable\", \"colArrayTextNotNull\", \"colArrayIntSyntax1\", \"colArrayIntSyntax2\", " +
                "\"colArrayDate\", \"colArrayReal\", \"colArrayNumeric\", \"colArrayDoublePrecision\", \"colArrayTimestamptz\", \"colArrayTimestamp\" FROM \"" + ALL_DATA_TYPES_TABLE_NAME + "\" ORDER BY \"colInteger\"";

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), "More records found in database than expected");
                GenericData.Record expected = expectedRecords.get(recordIndex);

                assertEquals(expected.get("colInteger"), rs.getInt("colInteger"), "ColInteger mismatch at index " + recordIndex);
                assertEquals(expected.get("colBigint"), rs.getObject("colBigint", Long.class), "ColBigint mismatch at index " + recordIndex);

                Object expectedNumeric = expected.get("colNumeric");
                if (expectedNumeric != null) {
                    BigDecimal expectedDecimal = bytesToDecimal((ByteBuffer) expectedNumeric);
                    assertEquals(0, expectedDecimal.compareTo(rs.getBigDecimal("colNumeric")), "ColNumeric mismatch at index " + recordIndex);
                } else {
                    assertNull(rs.getObject("colNumeric"), "ColNumeric should be null at index " + recordIndex);
                }

                assertEquals(expected.get("colReal"), rs.getObject("colReal", Float.class), "ColReal mismatch at index " + recordIndex);
                assertEquals(expected.get("colDoublePrecision"), rs.getObject("colDoublePrecision", Double.class), "ColDoublePrecision mismatch at index " + recordIndex);
                assertEquals(expected.get("colText"), rs.getString("colText"), "ColText mismatch at index " + recordIndex);
                assertEquals(expected.get("colBoolean"), rs.getObject("colBoolean", Boolean.class), "ColBoolean mismatch at index " + recordIndex);

                Object expectedDate = expected.get("colDate");
                if (expectedDate != null) {
                    LocalDate expectedLocalDate = LocalDate.ofEpochDay((Integer) expectedDate);
                    assertEquals(expectedLocalDate, rs.getDate("colDate").toLocalDate(), "ColDate mismatch at index " + recordIndex);
                } else {
                    assertNull(rs.getDate("colDate"), "ColDate should be null at index " + recordIndex);
                }

                Object expectedTimestamp = expected.get("colTimestamp");
                if (expectedTimestamp != null) {
                    LocalDateTime expectedLdt = epochMicrosToLocalDateTime((Long) expectedTimestamp);
                    assertEquals(expectedLdt, rs.getTimestamp("colTimestamp").toLocalDateTime(), "ColTimestamp mismatch at index " + recordIndex);
                } else {
                    assertNull(rs.getTimestamp("colTimestamp"), "ColTimestamp should be null at index " + recordIndex);
                }

                Object expectedTimestamptz = expected.get("colTimestamptz");
                if (expectedTimestamptz != null) {
                    Instant expectedInstant = Instant.ofEpochMilli((Long) expectedTimestamptz);
                    assertEquals(expectedInstant, rs.getTimestamp("colTimestamptz").toInstant(), "ColTimestamptz mismatch at index " + recordIndex);
                } else {
                    assertNull(rs.getTimestamp("colTimestamptz"), "ColTimestamptz should be null at index " + recordIndex);
                }

                Object expectedBytea = expected.get("colBytea");
                if (expectedBytea != null) {
                    byte[] expectedBytes = toByteArray(expectedBytea);
                    assertArrayEquals(expectedBytes, rs.getBytes("colBytea"), "ColBytea mismatch at index " + recordIndex);
                } else {
                    assertNull(rs.getBytes("colBytea"), "ColBytea should be null at index " + recordIndex);
                }

                verifyStringArray("colArrayTextNullable", (List<Object>) expected.get("colArrayTextNullable"), rs.getString("colArrayTextNullable"), recordIndex);
                verifyStringArray("colArrayTextNotNull", (List<Object>) expected.get("colArrayTextNotNull"), rs.getString("colArrayTextNotNull"), recordIndex);
                verifyIntArray("colArrayIntSyntax1", (List<Object>) expected.get("colArrayIntSyntax1"), rs.getString("colArrayIntSyntax1"), recordIndex);
                verifyIntArray("colArrayIntSyntax2", (List<Object>) expected.get("colArrayIntSyntax2"), rs.getString("colArrayIntSyntax2"), recordIndex);
                verifyDateArray("colArrayDate", (List<Object>) expected.get("colArrayDate"), rs.getString("colArrayDate"), recordIndex);
                verifyFloatArray("colArrayReal", (List<Object>) expected.get("colArrayReal"), rs.getString("colArrayReal"), recordIndex);
                verifyDecimalArray("colArrayNumeric", (List<Object>) expected.get("colArrayNumeric"), rs.getString("colArrayNumeric"), recordIndex);
                verifyDoubleArray("colArrayDoublePrecision", (List<Object>) expected.get("colArrayDoublePrecision"), rs.getString("colArrayDoublePrecision"), recordIndex);
                verifyTimestamptzArray("colArrayTimestamptz", (List<Object>) expected.get("colArrayTimestamptz"), rs.getString("colArrayTimestamptz"), recordIndex);
                verifyTimestampArray("colArrayTimestamp", (List<Object>) expected.get("colArrayTimestamp"), rs.getString("colArrayTimestamp"), recordIndex);

                recordIndex++;
            }
            assertEquals(expectedRecords.size(), recordIndex, "Expected " + expectedRecords.size() + " records but processed " + recordIndex);
        }
    }

    private byte[] toByteArray(Object obj) {
        if (obj instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer) obj;
            byte[] arr = new byte[bb.remaining()];
            bb.duplicate().get(arr);
            return arr;
        }
        return new byte[0];
    }

    private LocalDateTime epochMicrosToLocalDateTime(long epochMicros) {
        long epochSeconds = epochMicros / 1_000_000L;
        long microRemainder = epochMicros % 1_000_000L;
        Instant instant = Instant.ofEpochSecond(epochSeconds, microRemainder * 1_000L);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private List<String> parsePostgreSQLArray(String arrayString) {
        List<String> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return null;
        }
        String content = arrayString.substring(1, arrayString.length() - 1);
        if (content.trim().isEmpty()) {
            return result;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                String elem = current.toString().trim();
                result.add(elem.equals("NULL") ? null : (elem.startsWith("\"") && elem.endsWith("\"") ? elem.substring(1, elem.length() - 1) : elem));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        String elem = current.toString().trim();
        result.add(elem.equals("NULL") ? null : (elem.startsWith("\"") && elem.endsWith("\"") ? elem.substring(1, elem.length() - 1) : elem));
        return result;
    }

    private void verifyStringArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actual = parsePostgreSQLArray(actualArrayStr);
        List<String> expectedStr = expected.stream().map(o -> o == null ? null : o.toString()).collect(Collectors.toList());
        assertEquals(expectedStr, actual, fieldName + " mismatch at index " + idx);
    }

    private void verifyIntArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actualStr = parsePostgreSQLArray(actualArrayStr);
        List<Integer> actual = actualStr.stream().map(s -> s == null ? null : Integer.parseInt(s)).collect(Collectors.toList());
        List<Integer> expectedInt = expected.stream().map(o -> o == null ? null : (Integer) o).collect(Collectors.toList());
        assertEquals(expectedInt, actual, fieldName + " mismatch at index " + idx);
    }

    private void verifyDateArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actual = parsePostgreSQLArray(actualArrayStr);
        List<String> expectedStr = expected.stream()
                .map(o -> o == null ? null : LocalDate.ofEpochDay((Integer) o).toString())
                .collect(Collectors.toList());
        assertEquals(expectedStr, actual, fieldName + " mismatch at index " + idx);
    }

    private void verifyFloatArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actualStr = parsePostgreSQLArray(actualArrayStr);
        List<Float> actual = actualStr.stream().map(s -> s == null ? null : Float.parseFloat(s)).collect(Collectors.toList());
        List<Float> expectedFloat = expected.stream().map(o -> o == null ? null : (Float) o).collect(Collectors.toList());
        assertEquals(expectedFloat, actual, fieldName + " mismatch at index " + idx);
    }

    private void verifyDecimalArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actualStr = parsePostgreSQLArray(actualArrayStr);
        List<BigDecimal> actual = actualStr.stream().map(s -> s == null ? null : new BigDecimal(s)).collect(Collectors.toList());
        List<BigDecimal> expectedDecimal = expected.stream().map(o -> o == null ? null : bytesToDecimal((ByteBuffer) o)).collect(Collectors.toList());
        assertEquals(expectedDecimal.size(), actual.size(), fieldName + " size mismatch at index " + idx);
        for (int i = 0; i < expectedDecimal.size(); i++) {
            assertEquals(0, expectedDecimal.get(i).compareTo(actual.get(i)), fieldName + " element " + i + " mismatch at index " + idx);
        }
    }

    private void verifyDoubleArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actualStr = parsePostgreSQLArray(actualArrayStr);
        List<Double> actual = actualStr.stream().map(s -> s == null ? null : Double.parseDouble(s)).collect(Collectors.toList());
        List<Double> expectedDouble = expected.stream().map(o -> o == null ? null : (Double) o).collect(Collectors.toList());
        assertEquals(expectedDouble, actual, fieldName + " mismatch at index " + idx);
    }

    private void verifyTimestamptzArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actual = parsePostgreSQLArray(actualArrayStr);
        List<Instant> expectedInstants = expected.stream().map(o -> o == null ? null : Instant.ofEpochMilli((Long) o)).collect(Collectors.toList());
        List<Instant> actualInstants = actual.stream().map(s -> {
            if (s == null) return null;
            String normalized = s.replace(" ", "T");
            if (normalized.matches(".*[+-]\\d{2}$")) {
                normalized = normalized + ":00";
            }
            return OffsetDateTime.parse(normalized).toInstant();
        }).collect(Collectors.toList());
        assertEquals(expectedInstants, actualInstants, fieldName + " mismatch at index " + idx);
    }

    private void verifyTimestampArray(String fieldName, List<Object> expected, String actualArrayStr, int idx) {
        if (expected == null) {
            assertNull(actualArrayStr, fieldName + " should be null at index " + idx);
            return;
        }
        assertNotNull(actualArrayStr, fieldName + " should not be null at index " + idx);
        List<String> actualStr = parsePostgreSQLArray(actualArrayStr);
        List<LocalDateTime> actual = actualStr.stream()
                .map(s -> s == null ? null : LocalDateTime.parse(s.replace(" ", "T")))
                .collect(Collectors.toList());
        List<LocalDateTime> expectedLdt = expected.stream().map(o -> o == null ? null : epochMicrosToLocalDateTime((Long) o)).collect(Collectors.toList());
        assertEquals(expectedLdt.size(), actual.size(), fieldName + " size mismatch at index " + idx);
        for (int i = 0; i < expectedLdt.size(); i++) {
            LocalDateTime exp = expectedLdt.get(i);
            LocalDateTime act = actual.get(i);
            if (exp == null) {
                assertNull(act, fieldName + " element " + i + " should be null at index " + idx);
            } else {
                assertEquals(exp.getYear(), act.getYear(), fieldName + " year mismatch at index " + idx);
                assertEquals(exp.getMonth(), act.getMonth(), fieldName + " month mismatch at index " + idx);
                assertEquals(exp.getDayOfMonth(), act.getDayOfMonth(), fieldName + " day mismatch at index " + idx);
                assertEquals(exp.getHour(), act.getHour(), fieldName + " hour mismatch at index " + idx);
                assertEquals(exp.getMinute(), act.getMinute(), fieldName + " minute mismatch at index " + idx);
                assertEquals(exp.getSecond(), act.getSecond(), fieldName + " second mismatch at index " + idx);
            }
        }
    }
}
