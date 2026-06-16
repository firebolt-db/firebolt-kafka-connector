package com.firebolt.kafka.connect.integration.json.schema;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.AllDataTypesTestRecord;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class AllDataTypesSchemaSerializerTest extends SchemaBaseIntegrationTest {

    // All data types test constants
    private String ALL_DATA_TYPES_TABLE_NAME = generateTableName("all_data_types_test_table");
    private String ALL_DATA_TYPES_TOPIC_NAME = generateTopicName("all-data-types-test-topic");
    private String ALL_DATA_TYPES_SCHEMA_SUBJECT = ALL_DATA_TYPES_TOPIC_NAME + "-value";

    private static final DateFormat ISO_8601_DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd");

    private Producer<String, AllDataTypesTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("all-data-types-test-connector");

    }
    
    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }

        // Clean up test resources
        cleanupTestResources(ALL_DATA_TYPES_TABLE_NAME, ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_SCHEMA_SUBJECT);

        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("ingestionTypesWithOrWithoutNulls")
    void testAllDataTypesJsonSchemaSerializationAndKafkaConnectProcessing(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for all data types", testDescription);

        // Setup test resources using centralized method
        setupTestResources(ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_TABLE_NAME, ALL_DATA_TYPES_SCHEMA_SUBJECT,
                allDataTypesTableSchema(), allDataTypesJsonSchema(), connectorOverrides);

        producer = initializeJsonProducer(includeNulls);
        
        // Generate 5 test messages with different data patterns
        List<AllDataTypesTestRecord> testRecords = generateAllDataTypesTestRecords();

        // Publish messages to Kafka using JSON serialization
        publishAllDataTypesMessages(testRecords);

        // Wait for connector to process messages
        waitForDataInFirebolt(ALL_DATA_TYPES_TABLE_NAME, testRecords.size());

        // Verify data was written to Firebolt table
        verifyAllDataTypesRecordsInFirebolt(testRecords);
    }

    private Supplier<String> allDataTypesTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
        // Numeric types
        "\"colInteger\" INTEGER NOT NULL, " +
                "\"colBigint\" BIGINT, " +
                "\"colNumeric\" NUMERIC(38,9), " +
                "\"colReal\" REAL, " +
                "\"colDoublePrecision\" DOUBLE PRECISION, " +

                // Boolean type
                "\"colBoolean\" BOOLEAN, " +

                // String type
                "\"colText\" TEXT, " +

                // Date and timestamp types
                "\"colDate\" DATE, " +
                "\"colTimestamp\" TIMESTAMP, " +
                "\"colTimestamptz\" TIMESTAMPTZ, " +

                // Array types (various syntaxes and element types)
                "\"colArrayTextNullable\" ARRAY(TEXT NULL), " +
                "\"colArrayTextNotNull\" ARRAY(TEXT NOT NULL), " +
                "\"colArrayIntSyntax1\" ARRAY(INTEGER), " +
                "\"colArrayIntSyntax2\" INTEGER[], " +
                "\"colArrayDate\" ARRAY(DATE), " +
                "\"colArrayReal\" ARRAY(REAL), " +
                "\"colArrayNumeric\" ARRAY(NUMERIC), " +
                "\"colArrayDoublePrecision\" ARRAY(DOUBLE PRECISION), " +
                "\"colArrayTimestamptz\" ARRAY(TIMESTAMPTZ), " +
                "\"colArrayTimestamp\" ARRAY(TIMESTAMP) "
                + ");";
    }
    
    /**
     * Generates test records for all data types testing.
     */
    private List<AllDataTypesTestRecord> generateAllDataTypesTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidAllDataTypesTestRecord(1)
                .build(),

            // Record with edge case values
            aValidAllDataTypesTestRecord(2)
                .colBigint(Long.MAX_VALUE)
                .colNumeric(new BigDecimal("99999999999999999999999999999.999999999"))
                .colReal(98765.4321f)
                .colDoublePrecision(1.7976931348623157E300)
                .colText("Edge Case Test Data with very long text that might exceed normal limits")
                .colBoolean(false)
                .colDate(createDate(2099, Calendar.DECEMBER, 31))
                .colTimestamp(LocalDateTime.of(2099, 12, 31, 23, 59, 59, 999999000))
                .colTimestamptz(OffsetDateTime.of(2099, 12, 31, 23, 59, 59, 999999000, ZoneOffset.UTC))
                .build(),

            // Record with nullable values
            aValidAllDataTypesTestRecord(3)
                .colBigint(null)
                .colNumeric(null)
                .colReal(null)
                .colDoublePrecision(null)
                .colText(null)
                .colBoolean(null)
                .colDate(null)
                .colTimestamp(null)
                .colTimestamptz(null)
                .colArrayTextNullable(null)
                .colArrayTextNotNull(null)
                .colArrayIntSyntax1(null)
                .colArrayIntSyntax2(null)
                .colArrayDate(null)
                .colArrayReal(null)
                .colArrayNumeric(null)
                .colArrayDoublePrecision(null)
                .colArrayTimestamptz(null)
                .colArrayTimestamp(null)
                .build(),

            // Record with geographic sample data
            aValidAllDataTypesTestRecord(4)
                .colText("San Francisco")
                .colArrayTextNullable(Arrays.asList("San Francisco", "New York", null, "London", "Tokyo"))
                .colArrayTextNotNull(Arrays.asList("California", "New York", "England", "Japan"))
                .colArrayIntSyntax1(Arrays.asList(37, 40, 51, 35))
                .colArrayIntSyntax2(Arrays.asList(774, 840, 130, 392))
                .colArrayDate(Arrays.asList(
                    createDate(2024, Calendar.JANUARY, 1),
                    createDate(2024, Calendar.JANUARY, 2),
                    createDate(2024, Calendar.JANUARY, 3)
                ))
                .colArrayReal(Arrays.asList(37.7749f, 40.7128f, 51.5074f, 35.6762f))
                .build(),

            // Record with variety of data patterns
            aValidAllDataTypesTestRecord(5)
                .colBigint(-1000L)
                .colNumeric(new BigDecimal("-12345678901234567890123456789.123456789"))
                .colReal(-1.5f)
                .colDoublePrecision(-1.23456789)
                .colText("Variety Test Data with special characters: !@#$%^&*()")
                .colBoolean(true)
                .colDate(createDate(1970, Calendar.JANUARY, 1))
                .colTimestamp(LocalDateTime.of(2000, 1, 1, 0, 0, 30, 0))
                .colTimestamptz(OffsetDateTime.of(2000, 1, 1, 0, 0, 35, 0, ZoneOffset.UTC))
                .colArrayNumeric(Arrays.asList(
                    new BigDecimal("100.123456789"),
                    new BigDecimal("200.987654321"),
                    new BigDecimal("300.555555555")
                ))
                .colArrayDoublePrecision(Arrays.asList(1.11111, 2.22222, 3.33333, 4.44444))
                .colArrayTimestamptz(Arrays.asList(
                    OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC),
                    OffsetDateTime.of(2024, 1, 2, 13, 30, 20, 0, ZoneOffset.UTC),
                    OffsetDateTime.of(2024, 1, 3, 15, 45, 30, 0, ZoneOffset.UTC)
                ))
                .colArrayTimestamp(Arrays.asList(
                    LocalDateTime.of(2024, 1, 1, 12, 0, 25, 0),
                    LocalDateTime.of(2024, 1, 2, 13, 30, 25, 0),
                    LocalDateTime.of(2024, 1, 3, 15, 45, 30, 0)
                ))
                .build()
        );
    }

    /**
     * Helper to create java.util.Date at midnight UTC for a given Y/M/D.
     */
    private Date createDate(int year, int monthConstant, int dayOfMonth) {
        Calendar c = Calendar.getInstance();
        c.set(year, monthConstant, dayOfMonth, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    /**
     * Helper method to create a valid AllDataTypesTestRecord with default values.
     */
    private AllDataTypesTestRecord.AllDataTypesTestRecordBuilder aValidAllDataTypesTestRecord(int colInteger) {
        return AllDataTypesTestRecord.builder()
            // Numeric types
            .colInteger(colInteger)
            .colBigint(1000L)
            .colNumeric(new BigDecimal("12345678901234567890123456789.123456789")) // Full NUMERIC(38,9) precision
            .colReal(1.5f)
            .colDoublePrecision(1.23456789)
            
            // Boolean type
            .colBoolean(true)
            
            // String type
            .colText("Basic Test Data")
            
            // Date and timestamp types
            .colDate(createDate(2024, Calendar.JANUARY, 1))
            .colTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0))
            .colTimestamptz(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC))
            
            // Array type with nullable elements
            .colArrayTextNullable(Arrays.asList("apple", null, "banana", "cherry"))

            // Array type with non-null elements only
            .colArrayTextNotNull(Arrays.asList("apple", "banana", "cherry", "date"))

            // Integer array types
            .colArrayIntSyntax1(Arrays.asList(1, 2, 3, 4, 5))
            .colArrayIntSyntax2(Arrays.asList(10, 20, 30, 40, 50))

            // Date and Real array types
            .colArrayDate(Arrays.asList(
                createDate(2024, 1, 1),
                createDate(2024, 1, 2),
                createDate(2024, 1, 3)
            ))
            .colArrayReal(Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f))

            // New array types
            .colArrayNumeric(Arrays.asList(
                new BigDecimal("100.123456789"),
                new BigDecimal("200.987654321"),
                new BigDecimal("300.555555555")
            ))
            .colArrayDoublePrecision(Arrays.asList(1.11111, 2.22222, 3.33333, 4.44444))
            .colArrayTimestamptz(Arrays.asList(
                OffsetDateTime.of(2024, 1, 1, 12, 0, 10, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2024, 1, 2, 13, 30, 10, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2024, 1, 3, 15, 45, 30, 0, ZoneOffset.UTC)
            ))
            .colArrayTimestamp(Arrays.asList(
                LocalDateTime.of(2024, 1, 1, 12, 0, 10, 0),
                LocalDateTime.of(2024, 1, 2, 13, 30, 10, 0),
                LocalDateTime.of(2024, 1, 3, 15, 45, 30, 0)
            ));
    }
    
    /**
     * Publishes all data types messages to Kafka topic using JSON Schema serialization.
     */
    private void publishAllDataTypesMessages(List<AllDataTypesTestRecord> records) throws Exception {
        for (AllDataTypesTestRecord record : records) {
            ProducerRecord<String, AllDataTypesTestRecord> producerRecord =
                new ProducerRecord<>(ALL_DATA_TYPES_TOPIC_NAME, String.valueOf(record.getColInteger()), record);

            producer.send(producerRecord).get(); // Wait for each message to be sent
        }
    }
    
    /**
     * Verifies that all data types records were properly written to Firebolt.
     */
    private void verifyAllDataTypesRecordsInFirebolt(List<AllDataTypesTestRecord> expectedRecords) throws SQLException {

        // Verify specific records by checking the integer column (which is unique)
        String selectQuery = "SELECT \"colInteger\", \"colBigint\", \"colNumeric\", \"colReal\", \"colDoublePrecision\", \"colBoolean\", \"colText\", \"colDate\", " +
                "\"colTimestamp\", \"colTimestamptz\", \"colArrayTextNullable\", \"colArrayTextNotNull\", \"colArrayIntSyntax1\", \"colArrayIntSyntax2\", " +
                "\"colArrayDate\", \"colArrayReal\", \"colArrayNumeric\", \"colArrayDoublePrecision\", \"colArrayTimestamptz\", \"colArrayTimestamp\" FROM \"" + ALL_DATA_TYPES_TABLE_NAME + "\" ORDER BY \"colInteger\"";
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                AllDataTypesTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify key fields
                Integer actualColInteger = rs.getInt("colInteger");
                Long actualColBigint = rs.getObject("colBigint", Long.class);
                BigDecimal actualColNumeric = rs.getBigDecimal("colNumeric");
                Float actualColReal = rs.getObject("colReal", Float.class);
                Double actualColDoublePrecision = rs.getObject("colDoublePrecision", Double.class);
                String actualColText = rs.getString("colText");
                Boolean actualColBoolean = rs.getObject("colBoolean", Boolean.class);
                java.sql.Date actualColDate = rs.getDate("colDate");
                java.sql.Timestamp actualColTimestamp = rs.getTimestamp("colTimestamp");
                String actualColArrayTextNullable = rs.getString("colArrayTextNullable");
                String actualColArrayTextNotNull = rs.getString("colArrayTextNotNull");
                String actualColArrayIntSyntax1 = rs.getString("colArrayIntSyntax1");
                String actualColArrayIntSyntax2 = rs.getString("colArrayIntSyntax2");
                String actualColArrayDate = rs.getString("colArrayDate");
                String actualColArrayReal = rs.getString("colArrayReal");
                String actualColArrayNumeric = rs.getString("colArrayNumeric");
                String actualColArrayDoublePrecision = rs.getString("colArrayDoublePrecision");
                String actualColArrayTimestamptz = rs.getString("colArrayTimestamptz");
                String actualColArrayTimestamp = rs.getString("colArrayTimestamp");

                // For timestamptz, we need to handle the timestamp conversion
                OffsetDateTime actualColTimestamptz = rs.getTimestamp("colTimestamptz") != null ?
                        rs.getTimestamp("colTimestamptz").toInstant().atOffset(ZoneOffset.UTC) : null;
                
                assertEquals(expected.getColInteger(), actualColInteger, 
                    "ColInteger mismatch at index " + recordIndex);
                assertEquals(expected.getColBigint(), actualColBigint,
                    "ColBigint mismatch at index " + recordIndex);
                assertEqualsBigDecimal(expected.getColNumeric(), actualColNumeric, recordIndex);
                assertEquals(expected.getColReal(), actualColReal, 
                    "ColReal mismatch at index " + recordIndex);
                assertEquals(expected.getColDoublePrecision(), actualColDoublePrecision, 
                    "ColDoublePrecision mismatch at index " + recordIndex);
                assertEquals(expected.getColText(), actualColText, 
                    "ColText mismatch at index " + recordIndex);
                assertEquals(expected.getColBoolean(), actualColBoolean, 
                    "ColBoolean mismatch at index " + recordIndex);
                
                // Verify colDate field by ISO string compare
                if (actualColDate != null && expected.getColDate() != null) {
                    String actualIso = actualColDate.toString();
                    String expectedIso = ISO_8601_DATE_FORMAT.format(expected.getColDate());
                    assertEquals(expectedIso, actualIso,
                        "ColDate mismatch at index " + recordIndex);
                }
                
                // Verify colTimestamp field (convert java.sql.Timestamp to LocalDateTime for comparison)
                if (actualColTimestamp != null && expected.getColTimestamp() != null) {
                    java.time.LocalDateTime actualLocalDateTime = actualColTimestamp.toLocalDateTime();
                    // Connect Timestamp is millisecond precision; truncate expected accordingly
                    LocalDateTime expectedMillis = expected.getColTimestamp()
                            .truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
                    assertEquals(expectedMillis, actualLocalDateTime,
                        "ColTimestamp mismatch at index " + recordIndex);
                }

                // Verify timestamptz field (convert to OffsetDateTime for comparison)
                if (actualColTimestamptz != null && expected.getColTimestamptz() != null) {
                    java.time.Instant actualInstant = actualColTimestamptz.toInstant();
                    // Connect Timestamp is millisecond precision; truncate expected accordingly
                    java.time.Instant expectedInstant = expected.getColTimestamptz().toInstant()
                            .truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
                    assertEquals(expectedInstant, actualInstant,
                        "ColTimestamptz mismatch at index " + recordIndex);
                }
                
                // Verify colArrayTextNullable field (parse PostgreSQL array format)
                if (actualColArrayTextNullable != null && expected.getColArrayTextNullable() != null) {
                    try {
                        // Parse PostgreSQL array format: {apple,NULL,banana,cherry}
                        List<String> actualArray = parsePostgreSQLArray(actualColArrayTextNullable);
                        assertEquals(expected.getColArrayTextNullable(), actualArray,
                            "ColArrayTextNullable mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTextNullable PostgreSQL array: {}", actualColArrayTextNullable, e);
                        throw new RuntimeException("Failed to parse colArrayTextNullable PostgreSQL array", e);
                    }
                } else if (actualColArrayTextNullable == null && expected.getColArrayTextNullable() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayTextNullable and expected.getColArrayTextNullable() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayTextNullable(), actualColArrayTextNullable,
                        "ColArrayTextNullable null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayTextNotNull field (parse PostgreSQL array format)
                if (actualColArrayTextNotNull != null && expected.getColArrayTextNotNull() != null) {
                    try {
                        // Parse PostgreSQL array format: {apple,NULL,banana,cherry}
                        List<String> actualArray = parsePostgreSQLArray(actualColArrayTextNotNull);
                        assertEquals(expected.getColArrayTextNotNull(), actualArray,
                            "ColArrayTextNotNull mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTextNotNull PostgreSQL array: {}", actualColArrayTextNotNull, e);
                        throw new RuntimeException("Failed to parse colArrayTextNotNull PostgreSQL array", e);
                    }
                } else if (actualColArrayTextNotNull == null && expected.getColArrayTextNotNull() == null) {
                    log.debug("Both actualColArrayTextNotNull and expected.getColArrayTextNotNull() are null");
                } else {
                    assertEquals(expected.getColArrayTextNotNull(), actualColArrayTextNotNull,
                        "ColArrayTextNotNull null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayIntSyntax1 field (parse PostgreSQL array format)
                if (actualColArrayIntSyntax1 != null && expected.getColArrayIntSyntax1() != null) {
                    try {
                        // Parse PostgreSQL array format: {1,2,3}
                        List<Integer> actualArray = parsePostgreSQLArray(actualColArrayIntSyntax1).stream()
                                .map(Integer::parseInt)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayIntSyntax1(), actualArray,
                            "ColArrayIntSyntax1 mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayIntSyntax1 PostgreSQL array: {}", actualColArrayIntSyntax1, e);
                        throw new RuntimeException("Failed to parse colArrayIntSyntax1 PostgreSQL array", e);
                    }
                } else if (actualColArrayIntSyntax1 == null && expected.getColArrayIntSyntax1() == null) {
                    log.debug("Both actualColArrayIntSyntax1 and expected.getColArrayIntSyntax1() are null");
                } else {
                    assertEquals(expected.getColArrayIntSyntax1(), actualColArrayIntSyntax1,
                        "ColArrayIntSyntax1 null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayIntSyntax2 field (parse PostgreSQL array format)
                if (actualColArrayIntSyntax2 != null && expected.getColArrayIntSyntax2() != null) {
                    try {
                        // Parse PostgreSQL array format: {1,2,3}
                        List<Integer> actualArray = parsePostgreSQLArray(actualColArrayIntSyntax2).stream()
                                .map(Integer::parseInt)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayIntSyntax2(), actualArray,
                            "ColArrayIntSyntax2 mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayIntSyntax2 PostgreSQL array: {}", actualColArrayIntSyntax2, e);
                        throw new RuntimeException("Failed to parse colArrayIntSyntax2 PostgreSQL array", e);
                    }
                } else if (actualColArrayIntSyntax2 == null && expected.getColArrayIntSyntax2() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayIntSyntax2 and expected.getColArrayIntSyntax2() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayIntSyntax2(), actualColArrayIntSyntax2,
                        "ColArrayIntSyntax2 null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayDate field (parse PostgreSQL array format)
                if (actualColArrayDate != null && expected.getColArrayDate() != null) {
                    try {
                        // Parse PostgreSQL array format: {"2024-01-01","2024-01-02","2024-01-03"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayDate);
                        List<String> actualArray = actualStringArray;
                        List<String> expectedArray = expected.getColArrayDate().stream()
                                .map(d -> ISO_8601_DATE_FORMAT.format(d))
                                .collect(Collectors.toList());
                        assertEquals(expectedArray, actualArray,
                            "ColArrayDate mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayDate PostgreSQL array: {}", actualColArrayDate, e);
                        throw new RuntimeException("Failed to parse colArrayDate PostgreSQL array", e);
                    }
                } else if (actualColArrayDate == null && expected.getColArrayDate() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayDate and expected.getColArrayDate() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayDate(), actualColArrayDate,
                        "ColArrayDate null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayReal field (parse PostgreSQL array format)
                if (actualColArrayReal != null && expected.getColArrayReal() != null) {
                    try {
                        // Parse PostgreSQL array format: {1.1,2.2,3.3}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayReal);
                        List<Float> actualArray = actualStringArray.stream()
                                .map(Float::parseFloat)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayReal(), actualArray,
                            "ColArrayReal mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayReal PostgreSQL array: {}", actualColArrayReal, e);
                        throw new RuntimeException("Failed to parse colArrayReal PostgreSQL array", e);
                    }
                } else if (actualColArrayReal == null && expected.getColArrayReal() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayReal and expected.getColArrayReal() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayReal(), actualColArrayReal,
                        "ColArrayReal null mismatch at index " + recordIndex);
                }

                // Verify colArrayNumeric field (parse PostgreSQL array format)
                if (actualColArrayNumeric != null && expected.getColArrayNumeric() != null) {
                    try {
                        // Parse PostgreSQL array format: {"100.123456789","200.987654321"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayNumeric);
                        List<BigDecimal> actualArray = actualStringArray.stream()
                                .map(BigDecimal::new)
                                .collect(Collectors.toList());
                        
                        // Compare BigDecimal arrays using compareTo() to ignore scale differences
                        assertEquals(expected.getColArrayNumeric().size(), actualArray.size(),
                            "ColArrayNumeric size mismatch at index " + recordIndex);
                        
                        for (int i = 0; i < expected.getColArrayNumeric().size(); i++) {
                            BigDecimal expectedValue = expected.getColArrayNumeric().get(i);
                            BigDecimal actualValue = actualArray.get(i);
                            assertEquals(0, expectedValue.compareTo(actualValue),
                                "ColArrayNumeric value mismatch at index " + recordIndex + ", element " + i + 
                                ": expected " + expectedValue + " but was " + actualValue);
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayNumeric PostgreSQL array: {}", actualColArrayNumeric, e);
                        throw new RuntimeException("Failed to parse colArrayNumeric PostgreSQL array", e);
                    }
                } else if (actualColArrayNumeric == null && expected.getColArrayNumeric() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayNumeric and expected.getColArrayNumeric() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayNumeric(), actualColArrayNumeric,
                        "ColArrayNumeric null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayDoublePrecision field (parse PostgreSQL array format)
                if (actualColArrayDoublePrecision != null && expected.getColArrayDoublePrecision() != null) {
                    try {
                        // Parse PostgreSQL array format: {1.11111,2.22222,3.33333}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayDoublePrecision);
                        List<Double> actualArray = actualStringArray.stream()
                                .map(Double::parseDouble)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayDoublePrecision(), actualArray,
                            "ColArrayDoublePrecision mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayDoublePrecision PostgreSQL array: {}", actualColArrayDoublePrecision, e);
                        throw new RuntimeException("Failed to parse colArrayDoublePrecision PostgreSQL array", e);
                    }
                } else if (actualColArrayDoublePrecision == null && expected.getColArrayDoublePrecision() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayDoublePrecision and expected.getColArrayDoublePrecision() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayDoublePrecision(), actualColArrayDoublePrecision,
                        "ColArrayDoublePrecision null mismatch at index " + recordIndex);
                }

                // Verify colArrayTimestamptz field (parse PostgreSQL array format to OffsetDateTime)
                if (actualColArrayTimestamptz != null && expected.getColArrayTimestamptz() != null) {
                    try {
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayTimestamptz);
                        List<OffsetDateTime> actualArray = actualStringArray.stream()
                                .map(this::normalizePostgreSQLTimestamp)
                                .map(OffsetDateTime::parse)
                                .collect(Collectors.toList());
                        
                        // Compare as Instant values since Firebolt normalizes all timestamps to UTC
                        List<java.time.Instant> expectedInstants = expected.getColArrayTimestamptz().stream()
                                .map(OffsetDateTime::toInstant)
                                .collect(Collectors.toList());
                        List<java.time.Instant> actualInstants = actualArray.stream()
                                .map(OffsetDateTime::toInstant)
                                .collect(Collectors.toList());
                                
                        assertEquals(expectedInstants, actualInstants,
                            "ColArrayTimestamptz mismatch at index " + recordIndex + " (comparing as Instant)");
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTimestamptz PostgreSQL array: {}", actualColArrayTimestamptz, e);
                        throw new RuntimeException("Failed to parse colArrayTimestamptz PostgreSQL array", e);
                    }
                } else if (actualColArrayTimestamptz == null && expected.getColArrayTimestamptz() == null) {
                    log.debug("Both actualColArrayTimestamptz and expected.getColArrayTimestamptz() are null");
                } else {
                    assertEquals(expected.getColArrayTimestamptz(), actualColArrayTimestamptz,
                        "ColArrayTimestamptz null mismatch at index " + recordIndex);
                }

                // Verify colArrayTimestamp field (parse PostgreSQL array format to LocalDateTime)
                if (actualColArrayTimestamp != null && expected.getColArrayTimestamp() != null) {
                    try {
                        // Parse PostgreSQL array format: {"2024-01-01T12:00:00","2024-01-02T13:30:00"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayTimestamp);
                        List<LocalDateTime> actualArray = actualStringArray.stream()
                                .map(s -> s.replace(" ", "T")) // Handle potential space formatting
                                .map(LocalDateTime::parse)
                                .collect(Collectors.toList());
                        
                        // Connect Timestamp is millisecond precision; truncate expected accordingly
                        List<LocalDateTime> expectedRounded = expected.getColArrayTimestamp().stream()
                                .map(ldt -> ldt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
                                .collect(Collectors.toList());

                        assertEquals(expectedRounded, actualArray,
                            "ColArrayTimestamp mismatch at index " + recordIndex + " (comparing with millisecond precision)");
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTimestamp PostgreSQL array: {}", actualColArrayTimestamp, e);
                        throw new RuntimeException("Failed to parse colArrayTimestamp PostgreSQL array", e);
                    }
                } else if (actualColArrayTimestamp == null && expected.getColArrayTimestamp() == null) {
                    log.debug("Both actualColArrayTimestamp and expected.getColArrayTimestamp() are null");
                } else {
                    assertEquals(expected.getColArrayTimestamp(), actualColArrayTimestamp,
                        "ColArrayTimestamp null mismatch at index " + recordIndex);
                }
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected " + expectedRecords.size() + " records but processed " + recordIndex);
        }
    }
    
    /**
     * Registers JSON schema for AllDataTypesTestRecord.
     */
    private Supplier<String> allDataTypesJsonSchema() {
        // Schema that matches the AllDataTypesTestRecord class structure
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"All Data Types Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"colInteger\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Integer field (NOT NULL)\"\n" +
                "    },\n" +
                "    \"colBigint\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\"}\n" +
                "      ],\n" +
                "      \"description\": \"Bigint field\"\n" +
                "    },\n" +
                "    \"colNumeric\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"number\",\n" +
                "          \"connect.type\": \"bytes\",\n" +
                "          \"title\": \"org.apache.kafka.connect.data.Decimal\",\n" +
                "          \"connect.parameters\": { \"scale\": \"9\", \"connect.decimal.precision\": \"38\" }\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    \"colReal\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"connect.type\": \"float32\"}\n" +
                "      ],\n" +
                "      \"description\": \"Real field\"\n" +
                "    },\n" +
                "    \"colDoublePrecision\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"connect.type\": \"float64\"}\n" +
                "      ],\n" +
                "      \"description\": \"Double precision field\"\n" +
                "    },\n" +
                "    \"colBoolean\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"boolean\"}\n" +
                "      ],\n" +
                "      \"description\": \"Boolean field\"\n" +
                "    },\n" +
                "    \"colText\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"Text field\"\n" +
                "    },\n" +
                "    \"colDate\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int32\", \"title\": \"org.apache.kafka.connect.data.Date\"}\n" +
                "      ],\n" +
                "      \"description\": \"Date field\"\n" +
                "    },\n" +
                "    \"colTimestamp\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"integer\",\n" +
                "          \"connect.type\": \"int64\",\n" +
                "          \"connect.version\": 1,\n" +
                "          \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Timestamp field\"\n" +
                "    },\n" +
                "    \"colTimestamptz\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\"},\n" +
                "        {\n" +
                "          \"type\": \"integer\",\n" +
                "          \"connect.type\": \"int64\",\n" +
                "          \"connect.version\": 1,\n" +
                "          \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    \"colArrayTextNullable\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"string\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Text array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayTextNotNull\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"string\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Text array field with non-null elements\"\n" +
                "    },\n" +
                "    \"colArrayIntSyntax1\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"integer\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Integer array field (syntax 1) with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayIntSyntax2\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"integer\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Integer array field (syntax 2) with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayDate\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"integer\", \"connect.type\": \"int32\", \"title\": \"org.apache.kafka.connect.data.Date\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Date array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayReal\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"number\", \"connect.type\": \"float32\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Real array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayNumeric\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"number\", \"connect.type\": \"bytes\", \"title\": \"org.apache.kafka.connect.data.Decimal\", \"connect.parameters\": { \"scale\": \"9\", \"connect.decimal.precision\": \"38\" } }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Numeric array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayDoublePrecision\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"number\", \"connect.type\": \"float64\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Double precision array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayTimestamptz\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\n" +
                "                \"type\": \"integer\",\n" +
                "                \"connect.type\": \"int64\",\n" +
                "                \"connect.version\": 1,\n" +
                "                \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "              }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Timestamptz array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayTimestamp\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\n" +
                "                \"type\": \"integer\",\n" +
                "                \"connect.type\": \"int64\",\n" +
                "                \"connect.version\": 1,\n" +
                "                \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "              }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Timestamp array field with nullable elements\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"colInteger\"]\n" +
                "}";
    }

    /**
     * Parses a PostgreSQL array string into a List of strings.
     * Handles NULL values, quoted strings, and simple string arrays.
     * @param arrayString The PostgreSQL array string (e.g., {"San Francisco","New York",NULL,London,Tokyo})
     * @return A List of strings, with NULL values represented as null.
     */
    private List<String> parsePostgreSQLArray(String arrayString) {
        List<String> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return null; // Represent NULL as null
        }

        // Remove curly braces
        String content = arrayString.substring(1, arrayString.length() - 1);
        if (content.trim().isEmpty()) {
            return result; // Empty array
        }
        
        // Parse elements, handling quoted strings properly
        List<String> elements = parsePostgreSQLArrayElements(content);
        for (String element : elements) {
            String trimmedElement = element.trim();
            if (trimmedElement.equals("NULL")) {
                result.add(null); // PostgreSQL NULL becomes Java null
            } else if (trimmedElement.startsWith("\"") && trimmedElement.endsWith("\"")) {
                // Remove quotes from quoted strings
                result.add(trimmedElement.substring(1, trimmedElement.length() - 1));
            } else {
                result.add(trimmedElement);
            }
        }
        return result;
    }
    
    /**
     * Parses PostgreSQL array elements, properly handling quoted strings with commas.
     */
    private List<String> parsePostgreSQLArrayElements(String content) {
        List<String> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            
            if (c == '"' && (i == 0 || content.charAt(i-1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                elements.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        // Add the last element
        if (current.length() > 0) {
            elements.add(current.toString());
        }
        
        return elements;
    }
    
    /**
     * Normalizes PostgreSQL timestamp format to Java OffsetDateTime format.
     * PostgreSQL: "2024-01-01 12:00:00+00" -> Java: "2024-01-01T12:00:00+00:00"
     */
    private String normalizePostgreSQLTimestamp(String postgresTimestamp) {
        if (postgresTimestamp == null || postgresTimestamp.trim().isEmpty()) {
            return postgresTimestamp;
        }
        
        String normalized = postgresTimestamp.trim();
        
        // Replace space with 'T' between date and time
        normalized = normalized.replace(" ", "T");
        
        // Fix timezone format: convert +00 to +00:00, +02 to +02:00, etc.
        // Handle patterns like +00, -05, +09, etc.
        if (normalized.matches(".*[+-]\\d{2}$")) {
            normalized = normalized + ":00";
        }
        
        return normalized;
    }
    
    /**
     * Rounds a LocalDateTime to microsecond precision to match Firebolt's behavior.
     * Firebolt rounds nanoseconds to the nearest microsecond rather than truncating.
     */
    private LocalDateTime roundToMicroseconds(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        
        long nanos = dateTime.getNano();
        // Round to nearest microsecond (1000 nanoseconds = 1 microsecond)
        long microsInNanos = (nanos + 500) / 1000 * 1000;
        
        // Handle potential overflow to next second
        if (microsInNanos >= 1_000_000_000) {
            return dateTime.truncatedTo(java.time.temporal.ChronoUnit.SECONDS).plusSeconds(1);
        }
        
        return dateTime.withNano((int) microsInNanos);
    }
} 