package com.firebolt.kafka.connect.integration.protobuf;

import com.firebolt.kafka.connect.utils.TestTag;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Protobuf serialization via Confluent's ProtobufConverter.
 *
 * <p>Confluent's ProtobufConverter deserializes Protobuf messages into Kafka Connect Struct objects,
 * which are then handled by the existing SchemaBasedRecordConverter — no new converter code needed.
 *
 * <p>Records use DynamicMessage to avoid compiled .proto files, mirroring the Avro GenericRecord approach.
 */
@Slf4j
@Tag(TestTag.SERIALIZATION)
public class ProtobufAllDataTypesSerializerTest extends ProtobufBaseIntegrationTest {

    private static final String TABLE_NAME = "all_data_types_test_table_protobuf";
    private static final String TOPIC_NAME = "all-data-types-test-topic-protobuf";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("all-data-types-protobuf-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupProtobufTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAllDataTypesProtobufSerializationAndKafkaConnectProcessing(
            Map<String, String> connectorOverride, String testDescription) throws Exception {
        log.info("Running {} for all data types (Protobuf)", testDescription);

        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                tableSchema(), protobufSchema(), connectorOverride);

        // Parse the schema to get the Descriptor for DynamicMessage construction
        ProtobufSchema parsedSchema = new ProtobufSchema(protobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor descriptor = fileDescriptor.findMessageTypeByName("AllDataTypesRecord");

        List<DynamicMessage> testRecords = generateTestRecords(descriptor);

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (int i = 0; i < testRecords.size(); i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, String.valueOf(i + 1), testRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        verifyRecordsInFirebolt(testRecords, descriptor);
    }

    // ---------------------------------------------------------------------------
    // Table DDL
    // ---------------------------------------------------------------------------

    private Supplier<String> tableSchema() {
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
                "\"colArrayText\" ARRAY(TEXT NULL), " +
                "\"colArrayInt\" ARRAY(INTEGER), " +
                "\"colArrayDate\" ARRAY(DATE), " +
                "\"colArrayReal\" ARRAY(REAL), " +
                "\"colArrayNumeric\" ARRAY(NUMERIC), " +
                "\"colArrayDoublePrecision\" ARRAY(DOUBLE PRECISION), " +
                "\"colArrayTimestamptz\" ARRAY(TIMESTAMPTZ), " +
                "\"colArrayTimestamp\" ARRAY(TIMESTAMP) " +
                ");";
    }

    // ---------------------------------------------------------------------------
    // Protobuf schema definition
    //
    // Type mapping rationale:
    //   INT      -> int32  (passed through as Connect INT32)
    //   BIGINT   -> int64  (Connect INT64)
    //   REAL     -> float  (Connect FLOAT32)
    //   DOUBLE   -> double (Connect FLOAT64)
    //   TEXT     -> string (Connect STRING)
    //   DECIMAL  -> string (SchemaDecimalDataTypeConverter accepts STRING)
    //   DATE     -> string (ISO-8601; SchemaDateDataTypeConverter accepts STRING)
    //   TIMESTAMP/TIMESTAMPTZ -> google.protobuf.Timestamp
    //                           ProtobufConverter maps to Connect Timestamp (INT64 millis)
    //   BOOLEAN  -> bool   (Connect BOOLEAN)
    //   BYTEA    -> bytes  (Connect BYTES)
    //   ARRAY    -> repeated fields (Connect ARRAY)
    // ---------------------------------------------------------------------------

    private Supplier<String> protobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "import \"google/protobuf/timestamp.proto\";\n" +
                "message AllDataTypesRecord {\n" +
                "  int32 colInteger = 1;\n" +
                "  int64 colBigint = 2;\n" +
                "  string colNumeric = 3;\n" +
                "  float colReal = 4;\n" +
                "  double colDoublePrecision = 5;\n" +
                "  bool colBoolean = 6;\n" +
                "  string colText = 7;\n" +
                "  string colDate = 8;\n" +
                "  google.protobuf.Timestamp colTimestamp = 9;\n" +
                "  google.protobuf.Timestamp colTimestamptz = 10;\n" +
                "  bytes colBytea = 11;\n" +
                "  repeated string colArrayText = 12;\n" +
                "  repeated int32 colArrayInt = 13;\n" +
                "  repeated string colArrayDate = 14;\n" +
                "  repeated float colArrayReal = 15;\n" +
                "  repeated string colArrayNumeric = 16;\n" +
                "  repeated double colArrayDoublePrecision = 17;\n" +
                "  repeated google.protobuf.Timestamp colArrayTimestamptz = 18;\n" +
                "  repeated google.protobuf.Timestamp colArrayTimestamp = 19;\n" +
                "}\n";
    }

    // ---------------------------------------------------------------------------
    // Record construction
    // ---------------------------------------------------------------------------

    private List<DynamicMessage> generateTestRecords(Descriptor descriptor) {
        List<DynamicMessage> records = new ArrayList<>();

        // Record 1: typical values
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 1)
                .setField(descriptor.findFieldByName("colBigint"), 1000L)
                .setField(descriptor.findFieldByName("colNumeric"), "12345678901234567890123456789.123456789")
                .setField(descriptor.findFieldByName("colReal"), 1.5f)
                .setField(descriptor.findFieldByName("colDoublePrecision"), 1.23456789)
                .setField(descriptor.findFieldByName("colBoolean"), true)
                .setField(descriptor.findFieldByName("colText"), "Basic Test Data")
                .setField(descriptor.findFieldByName("colDate"), "2024-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("hello".getBytes()))
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "apple")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "banana")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "cherry")
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 1)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 2)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 3)
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-01")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-02")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-03")
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 1.1f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 2.2f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 3.3f)
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "100.123456789")
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "200.987654321")
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 1.11111)
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 2.22222)
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 10, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 2, 13, 30, 10, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 10, 0)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 2, 13, 30, 10, 0)))
                .build());

        // Record 2: edge case / max values
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 2)
                .setField(descriptor.findFieldByName("colBigint"), Long.MAX_VALUE)
                .setField(descriptor.findFieldByName("colNumeric"), "99999999999999999999999999999.999999999")
                .setField(descriptor.findFieldByName("colReal"), Float.MAX_VALUE)
                .setField(descriptor.findFieldByName("colDoublePrecision"), Double.MAX_VALUE)
                .setField(descriptor.findFieldByName("colBoolean"), false)
                .setField(descriptor.findFieldByName("colText"), "Edge Case Test Data with very long text that might exceed normal limits")
                .setField(descriptor.findFieldByName("colDate"), "2099-12-31")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2099, 12, 31, 23, 59, 59, 999000000)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2099, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("edge_case_binary_data".getBytes()))
                .build());

        // Record 3: proto3 zero-defaults for most fields.
        // Proto3 scalar defaults (0, false, "") arrive as zero values in the Connect Struct.
        // SchemaBasedRecordConverter passes them through as-is; the SQL/binary converters then
        // map them to their Firebolt equivalents (0 → 0, false → false, "" → NULL for NUMERIC/DATE
        // since an empty string is not a valid NUMERIC/DATE literal).
        // We therefore set colNumeric and colDate explicitly to avoid converter rejection of "", and
        // leave remaining string-typed scalars (colText, colBytea) unset — they arrive as "" / empty
        // bytes and map to empty string / empty bytea in Firebolt.
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 3)
                .setField(descriptor.findFieldByName("colNumeric"), "0")
                .setField(descriptor.findFieldByName("colDate"), "2000-01-01")
                .build());

        // Record 4: geographic sample data
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 4)
                .setField(descriptor.findFieldByName("colBigint"), 1000L)
                .setField(descriptor.findFieldByName("colNumeric"), "12345678901234567890123456789.123456789")
                .setField(descriptor.findFieldByName("colReal"), 1.5f)
                .setField(descriptor.findFieldByName("colDoublePrecision"), 1.23456789)
                .setField(descriptor.findFieldByName("colBoolean"), true)
                .setField(descriptor.findFieldByName("colText"), "San Francisco")
                .setField(descriptor.findFieldByName("colDate"), "2024-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("hello".getBytes()))
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "San Francisco")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "New York")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "London")
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 37)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 40)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 51)
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-01")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-02")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-03")
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 37.7749f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 40.7128f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 51.5074f)
                .build());

        // Record 5: negative / variety values
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 5)
                .setField(descriptor.findFieldByName("colBigint"), -1000L)
                .setField(descriptor.findFieldByName("colNumeric"), "-12345678901234567890123456789.123456789")
                .setField(descriptor.findFieldByName("colReal"), -1.5f)
                .setField(descriptor.findFieldByName("colDoublePrecision"), -1.23456789)
                .setField(descriptor.findFieldByName("colBoolean"), true)
                .setField(descriptor.findFieldByName("colText"), "Variety Test Data with special characters: !@#$%^&*()")
                .setField(descriptor.findFieldByName("colDate"), "1970-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2000, 1, 1, 0, 0, 30, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2000, 1, 1, 0, 0, 35, 0, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("variety_binary_data".getBytes()))
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "apple")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "banana")
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 1)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 2)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 3)
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "100.123456789")
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "200.987654321")
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 1.11111)
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 2.22222)
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 2, 13, 30, 20, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 25, 0)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 2, 13, 30, 25, 0)))
                .build());

        return records;
    }

    // ---------------------------------------------------------------------------
    // Verification
    // ---------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<DynamicMessage> expected, Descriptor descriptor) throws SQLException {
        String query =
                "SELECT \"colInteger\", \"colBigint\", \"colNumeric\", \"colReal\", \"colDoublePrecision\", " +
                "\"colBoolean\", \"colText\", \"colDate\", \"colTimestamp\", \"colTimestamptz\", \"colBytea\", " +
                "\"colArrayText\", \"colArrayInt\", \"colArrayDate\", \"colArrayReal\", " +
                "\"colArrayNumeric\", \"colArrayDoublePrecision\", \"colArrayTimestamptz\", \"colArrayTimestamp\" " +
                "FROM \"" + TABLE_NAME + "\" ORDER BY \"colInteger\"";

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(query)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expected.size(), "More records in DB than expected");
                DynamicMessage rec = expected.get(idx);

                // Scalar fields
                assertEquals(rec.getField(descriptor.findFieldByName("colInteger")), rs.getInt("colInteger"),
                        "colInteger mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colBigint")), rs.getObject("colBigint", Long.class),
                        "colBigint mismatch at " + idx);

                // NUMERIC stored as string in proto; Firebolt returns BigDecimal
                String expectedNumeric = (String) rec.getField(descriptor.findFieldByName("colNumeric"));
                BigDecimal actualNumeric = rs.getBigDecimal("colNumeric");
                if (expectedNumeric != null && !expectedNumeric.isEmpty()) {
                    assertNotNull(actualNumeric, "colNumeric should not be null at " + idx);
                    assertEquals(0, new BigDecimal(expectedNumeric).compareTo(actualNumeric),
                            "colNumeric mismatch at " + idx + ": expected " + expectedNumeric + " got " + actualNumeric);
                } else {
                    assertNull(actualNumeric, "colNumeric should be null at " + idx);
                }

                assertEquals(rec.getField(descriptor.findFieldByName("colReal")), rs.getObject("colReal", Float.class),
                        "colReal mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colDoublePrecision")), rs.getObject("colDoublePrecision", Double.class),
                        "colDoublePrecision mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colBoolean")), rs.getObject("colBoolean", Boolean.class),
                        "colBoolean mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colText")), rs.getString("colText"),
                        "colText mismatch at " + idx);

                // DATE: stored as ISO string in proto, verify as date
                String expectedDate = (String) rec.getField(descriptor.findFieldByName("colDate"));
                java.sql.Date actualDate = rs.getDate("colDate");
                if (expectedDate != null && !expectedDate.isEmpty()) {
                    assertNotNull(actualDate, "colDate should not be null at " + idx);
                    assertEquals(expectedDate, actualDate.toString(), "colDate mismatch at " + idx);
                } else {
                    assertNull(actualDate, "colDate should be null at " + idx);
                }

                // TIMESTAMP: proto Timestamp → ProtobufConverter → Connect Timestamp (millis) → SQL Timestamp
                com.google.protobuf.Timestamp expectedTs =
                        (com.google.protobuf.Timestamp) rec.getField(descriptor.findFieldByName("colTimestamp"));
                java.sql.Timestamp actualTs = rs.getTimestamp("colTimestamp");
                if (expectedTs != null && (expectedTs.getSeconds() != 0 || expectedTs.getNanos() != 0)) {
                    assertNotNull(actualTs, "colTimestamp should not be null at " + idx);
                    long expectedMillis = expectedTs.getSeconds() * 1000L + expectedTs.getNanos() / 1_000_000L;
                    assertEquals(expectedMillis, actualTs.getTime(), "colTimestamp mismatch at " + idx);
                } else {
                    assertNull(actualTs, "colTimestamp should be null at " + idx);
                }

                // TIMESTAMPTZ: same approach, compare as Instant
                com.google.protobuf.Timestamp expectedTstz =
                        (com.google.protobuf.Timestamp) rec.getField(descriptor.findFieldByName("colTimestamptz"));
                java.sql.Timestamp actualTstz = rs.getTimestamp("colTimestamptz");
                if (expectedTstz != null && (expectedTstz.getSeconds() != 0 || expectedTstz.getNanos() != 0)) {
                    assertNotNull(actualTstz, "colTimestamptz should not be null at " + idx);
                    Instant expectedInstant = Instant.ofEpochSecond(expectedTstz.getSeconds(), expectedTstz.getNanos());
                    assertEquals(expectedInstant, actualTstz.toInstant(), "colTimestamptz mismatch at " + idx);
                } else {
                    assertNull(actualTstz, "colTimestamptz should be null at " + idx);
                }

                // BYTEA: proto bytes → byte[]
                // Proto3 default ByteString.EMPTY may arrive as null or empty byte[] from Firebolt.
                ByteString expectedBytea = (ByteString) rec.getField(descriptor.findFieldByName("colBytea"));
                byte[] actualBytea = rs.getBytes("colBytea");
                if (expectedBytea != null && !expectedBytea.isEmpty()) {
                    assertNotNull(actualBytea, "colBytea should not be null at " + idx);
                    assertArrayEquals(expectedBytea.toByteArray(), actualBytea, "colBytea mismatch at " + idx);
                } else {
                    assertTrue(actualBytea == null || actualBytea.length == 0,
                            "colBytea should be null or empty at " + idx);
                }

                // Arrays
                List<String> expectedText = (List<String>) rec.getField(descriptor.findFieldByName("colArrayText"));
                verifyStringArray("colArrayText", expectedText, rs.getString("colArrayText"), idx);

                List<Integer> expectedInts = (List<Integer>) rec.getField(descriptor.findFieldByName("colArrayInt"));
                verifyIntArray("colArrayInt", expectedInts, rs.getString("colArrayInt"), idx);

                List<String> expectedDates = (List<String>) rec.getField(descriptor.findFieldByName("colArrayDate"));
                verifyStringArray("colArrayDate", expectedDates, rs.getString("colArrayDate"), idx);

                List<Float> expectedReals = (List<Float>) rec.getField(descriptor.findFieldByName("colArrayReal"));
                verifyFloatArray("colArrayReal", expectedReals, rs.getString("colArrayReal"), idx);

                List<String> expectedNumerics = (List<String>) rec.getField(descriptor.findFieldByName("colArrayNumeric"));
                verifyDecimalStringArray("colArrayNumeric", expectedNumerics, rs.getString("colArrayNumeric"), idx);

                List<Double> expectedDoubles = (List<Double>) rec.getField(descriptor.findFieldByName("colArrayDoublePrecision"));
                verifyDoubleArray("colArrayDoublePrecision", expectedDoubles, rs.getString("colArrayDoublePrecision"), idx);

                List<com.google.protobuf.Timestamp> expectedTstzArr =
                        (List<com.google.protobuf.Timestamp>) rec.getField(descriptor.findFieldByName("colArrayTimestamptz"));
                verifyTimestamptzArray("colArrayTimestamptz", expectedTstzArr, rs.getString("colArrayTimestamptz"), idx);

                List<com.google.protobuf.Timestamp> expectedTsArr =
                        (List<com.google.protobuf.Timestamp>) rec.getField(descriptor.findFieldByName("colArrayTimestamp"));
                verifyTimestampArray("colArrayTimestamp", expectedTsArr, rs.getString("colArrayTimestamp"), idx);

                idx++;
            }
            assertEquals(expected.size(), idx, "Expected " + expected.size() + " records but found " + idx);
        }
    }

    // ---------------------------------------------------------------------------
    // Array verification helpers
    // ---------------------------------------------------------------------------

    /**
     * Proto3 empty repeated fields default to [] and are stored as {} (empty array) in Firebolt,
     * not NULL. Accept both null and empty-array when expected is empty.
     */
    private void verifyStringArray(String field, List<String> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<String> actual = parsePostgreSQLArray(actualStr);
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyIntArray(String field, List<Integer> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<Integer> actual = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : Integer.parseInt(s))
                .collect(Collectors.toList());
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyFloatArray(String field, List<Float> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<Float> actual = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : Float.parseFloat(s))
                .collect(Collectors.toList());
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyDoubleArray(String field, List<Double> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<Double> actual = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : Double.parseDouble(s))
                .collect(Collectors.toList());
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyDecimalStringArray(String field, List<String> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<BigDecimal> actualDecimals = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : new BigDecimal(s))
                .collect(Collectors.toList());
        List<BigDecimal> expectedDecimals = expected.stream()
                .map(s -> s == null ? null : new BigDecimal(s))
                .collect(Collectors.toList());
        assertEquals(expectedDecimals.size(), actualDecimals.size(), field + " size mismatch at " + idx);
        for (int i = 0; i < expectedDecimals.size(); i++) {
            assertEquals(0, expectedDecimals.get(i).compareTo(actualDecimals.get(i)),
                    field + " element " + i + " mismatch at " + idx);
        }
    }

    private void verifyTimestamptzArray(
            String field, List<com.google.protobuf.Timestamp> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<String> actualStrings = parsePostgreSQLArray(actualStr);
        List<Instant> actualInstants = actualStrings.stream()
                .map(s -> {
                    if (s == null) return null;
                    String normalized = s.replace(" ", "T");
                    if (normalized.matches(".*[+-]\\d{2}$")) normalized = normalized + ":00";
                    return OffsetDateTime.parse(normalized).toInstant();
                }).collect(Collectors.toList());
        List<Instant> expectedInstants = expected.stream()
                .map(ts -> Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()))
                .collect(Collectors.toList());
        assertEquals(expectedInstants, actualInstants, field + " mismatch at " + idx);
    }

    private void verifyTimestampArray(
            String field, List<com.google.protobuf.Timestamp> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertTrue(actualStr == null || parsePostgreSQLArray(actualStr).isEmpty(),
                    field + " should be null or empty at " + idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<String> actualStrings = parsePostgreSQLArray(actualStr);
        List<LocalDateTime> actualLdts = actualStrings.stream()
                .map(s -> s == null ? null : LocalDateTime.parse(s.replace(" ", "T")))
                .collect(Collectors.toList());
        List<LocalDateTime> expectedLdts = expected.stream()
                .map(ts -> LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()), ZoneOffset.UTC))
                .collect(Collectors.toList());
        assertEquals(expectedLdts.size(), actualLdts.size(), field + " size mismatch at " + idx);
        for (int i = 0; i < expectedLdts.size(); i++) {
            LocalDateTime exp = expectedLdts.get(i);
            LocalDateTime act = actualLdts.get(i);
            if (exp == null) {
                assertNull(act, field + " element " + i + " should be null at " + idx);
            } else {
                assertEquals(exp.getYear(), act.getYear(), field + " year mismatch at " + idx);
                assertEquals(exp.getMonth(), act.getMonth(), field + " month mismatch at " + idx);
                assertEquals(exp.getDayOfMonth(), act.getDayOfMonth(), field + " day mismatch at " + idx);
                assertEquals(exp.getHour(), act.getHour(), field + " hour mismatch at " + idx);
                assertEquals(exp.getMinute(), act.getMinute(), field + " minute mismatch at " + idx);
                assertEquals(exp.getSecond(), act.getSecond(), field + " second mismatch at " + idx);
            }
        }
    }

    /** Builds a google.protobuf.Timestamp from a LocalDateTime (treated as UTC). */
    private com.google.protobuf.Timestamp toProtobufTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    /** Builds a google.protobuf.Timestamp from an OffsetDateTime. */
    private com.google.protobuf.Timestamp toProtobufTimestamp(OffsetDateTime odt) {
        Instant instant = odt.toInstant();
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    // ---------------------------------------------------------------------------
    // PostgreSQL array string parser (mirrors AllDataTypesAvroSchemaSerializerTest)
    // ---------------------------------------------------------------------------

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
                result.add(parseElement(current.toString().trim()));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(parseElement(current.toString().trim()));
        return result;
    }

    private String parseElement(String elem) {
        if (elem.equals("NULL")) return null;
        if (elem.startsWith("\"") && elem.endsWith("\"")) return elem.substring(1, elem.length() - 1);
        return elem;
    }
}

