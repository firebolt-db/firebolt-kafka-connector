package com.firebolt.kafka.connect.integration.json.schema;
import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.DoubleTestRecord;
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
public class DoubleSchemaSerializerTest extends SchemaBaseIntegrationTest {
    
    private static final String TABLE_NAME = generateTableName("double_test_table");
    private static final String TOPIC_NAME = generateTopicName("double-test-topic");
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, DoubleTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("double-serializer-test");
    }
    
    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }
        
        // Clean up test resources
        cleanupTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        
        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("ingestionTypesWithOrWithoutNulls")
    void testDoubleSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for double precision data type", testDescription);

        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                doubleTableSchema(), jsonDoubleSchema(), connectorOverrides);

        producer = initializeJsonProducer(includeNulls);

        List<DoubleTestRecord> testRecords = createTestRecords();

        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        verifyDoubleRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all scenarios for DOUBLE PRECISION.
     * DOUBLE has 15 decimal-digit precision, so we test edge cases around this limit.
     */
    private List<DoubleTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with maximum precision (15 decimal digits)
            aValidTestRecord(2)
                .requiredDouble(123456789012345.123456789012345)
                .optionalDouble(-987654321098765.987654321098765)
                .build(),

            // Record with minimum precision (very small numbers)
            aValidTestRecord(3)
                .requiredDouble(0.000000000000001)
                .optionalDouble(-0.000000000000001)
                .build(),

            // Record with null optional double
            aValidTestRecord(4)
                .optionalDouble(null)
                .build(),

            // Record with zero values
            aValidTestRecord(5)
                .requiredDouble(0.0)
                .optionalDouble(-0.0)
                .build(),

            // Record with very large but reasonable numbers (within Firebolt limits)
            aValidTestRecord(6)
                .requiredDouble(1.0E+15)  // 1 quadrillion
                .optionalDouble(-1.0E+15)
                .build(),

            // Record with very small but reasonable numbers (within Firebolt limits)
            aValidTestRecord(7)
                .requiredDouble(1.0E-15)  // 1 femtometer
                .optionalDouble(-1.0E-15)
                .build(),

            // Record with edge case lists (15 decimal precision)
            aValidTestRecord(8)
                .requiredListWithNullableElements(Arrays.asList(
                    123456789012345.123456789012345,
                    null,
                    -987654321098765.987654321098765,
                    0.000000000000001
                ))
                .requiredListWithNonNullElements(Arrays.asList(
                    1.234567890123456,
                    -2.345678901234567,
                    3.456789012345678,
                    4.567890123456789
                ))
                .build(),

            // Record with large but reasonable numbers (well within Firebolt limits)
            aValidTestRecord(9)
                .requiredDouble(1.0E+10)  // 10 billion
                .optionalDouble(-1.0E+10)
                .build(),

            // Record with small but reasonable numbers (well within Firebolt limits)
            aValidTestRecord(10)
                .requiredDouble(1.0E-10)  // 0.1 nanometer
                .optionalDouble(-1.0E-10)
                .build(),

            // Record with Double edge cases (MIN_VALUE and MAX_VALUE)
            aValidTestRecord(11)
                .requiredDouble(Double.MIN_VALUE)  // Smallest positive double value
                .optionalDouble(Double.MAX_VALUE)   // Largest double value
                .build(),

            // Record with mathematical constants
            aValidTestRecord(12)
                .requiredDouble(Math.PI)           // 3.141592653589793
                .optionalDouble(Math.E)            // 2.718281828459045
                .build(),

            // Record with edge case arrays containing extreme values (no infinity values)
            aValidTestRecord(13)
                .requiredListWithNullableElements(Arrays.asList(
                    Double.MIN_VALUE,
                    Double.MAX_VALUE,
                    null,
                    Double.MIN_NORMAL
                ))
                .requiredListWithNonNullElements(Arrays.asList(
                    Math.PI,
                    Math.E,
                    -Math.PI,
                    -Math.E
                ))
                .build(),

            // Record with scientific notation edge cases
            aValidTestRecord(14)
                .requiredDouble(1.7976931348623157E+308)  // Close to MAX_VALUE
                .optionalDouble(4.9E-324)                 // Close to MIN_VALUE
                .build(),

            // Record with precision boundary cases
            aValidTestRecord(15)
                .requiredDouble(9007199254740991.0)       // 2^53 - 1 (max safe integer in double)
                .optionalDouble(-9007199254740991.0)      // -(2^53 - 1)
                .build(),

            // Record with very large lists containing edge values
            aValidTestRecord(16)
                .requiredDouble(999.999999999999)
                .optionalDouble(-999.999999999999)
                .requiredListWithNullableElements(createLargeListWithNulls(5000))
                .requiredListWithNonNullElements(createLargeListWithoutNulls(5000))
                .optionalList(createOptionalLargeList(5000))
                .optionalListWithNonNullElements(createOptionalLargeList(3000))  // Different size for variety
                .build(),

            // Record with extreme precision test
            aValidTestRecord(17)
                .requiredDouble(1.2345678901234567890123456789)  // More precision than double can handle
                .optionalDouble(-1.2345678901234567890123456789)
                .build(),

            // Record with subnormal numbers (very close to zero)
            aValidTestRecord(18)
                .requiredDouble(Double.MIN_NORMAL)      // Smallest normal positive double
                .optionalDouble(-Double.MIN_NORMAL)     // Smallest normal negative double
                .build()
        );
    }
    
    private Supplier<String> doubleTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredDouble\" DOUBLE PRECISION NOT NULL, " +
                "\"optionalDouble\" DOUBLE PRECISION NULL, " +
                "\"optionalByte\" DOUBLE PRECISION NULL, " +
                "\"optionalShort\" DOUBLE PRECISION NULL, " +
                "\"optionalInt\" DOUBLE PRECISION NULL, " +
                "\"optionalLong\" DOUBLE PRECISION NULL, " +
                "\"optionalReal\" DOUBLE PRECISION NULL, " +
                "\"doubleFromString\" DOUBLE PRECISION NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(DOUBLE PRECISION NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(DOUBLE PRECISION NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(DOUBLE PRECISION NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(DOUBLE PRECISION NOT NULL) NULL" +
                ")";
    }
    
    private Supplier<String> jsonDoubleSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Double Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredDouble\": {\n" +
                "      \"type\": \"number\",\n" +
                "      \"connect.type\": \"float64\",\n" +
                "      \"description\": \"Required double field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalDouble\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"connect.type\": \"float64\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional double field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"optionalByte\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int8\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional byte mapped to DOUBLE PRECISION in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalShort\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int16\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional short mapped to DOUBLE PRECISION in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalInt\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int32\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional int mapped to DOUBLE PRECISION in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalLong\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int64\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional long mapped to DOUBLE PRECISION in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalReal\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"connect.type\": \"float32\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional real (float32) mapped to DOUBLE PRECISION in Firebolt\"\n" +
                "    },\n" +
                "    \"doubleFromString\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"Double value represented as string, mapped to DOUBLE PRECISION in Firebolt\"\n" +
                "    },\n" +
                "    \"requiredListWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"number\", \"connect.type\": \"float64\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required list where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredListWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"number\", \"connect.type\": \"float64\"},\n" +
                "      \"description\": \"Required list where individual elements cannot be null\"\n" +
                "    },\n" +
                "    \"optionalList\": {\n" +
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
                "      \"description\": \"Optional list - entire list can be null or omitted, and elements can be null\"\n" +
                "    },\n" +
                "    \"optionalListWithNonNullElements\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"number\", \"connect.type\": \"float64\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional list where individual elements cannot be null\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredDouble\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]\n" +
                "}";
    }
    
    /**
     * Publishes DoubleTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<DoubleTestRecord> records) throws Exception {
        for (DoubleTestRecord record : records) {
            String key = "double-test-key-" + record.getRecordId();
            ProducerRecord<String, DoubleTestRecord> producerRecord = 
                new ProducerRecord<>(TOPIC_NAME, key, record);
            
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message with key {}: {}", key, exception.getMessage());
                } else {
                    log.debug("Successfully sent message with key {} to partition {} at offset {}", 
                        key, metadata.partition(), metadata.offset());
                }
            }).get();
        }
        
        producer.flush();
    }
    
    /**
     * Verifies that the published double records exist in the Firebolt table with correct null handling.
     */
    private void verifyDoubleRecordsInFirebolt(List<DoubleTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredDouble\", \"optionalDouble\", " +
            "\"optionalByte\", \"optionalShort\", \"optionalInt\", \"optionalLong\", \"optionalReal\", \"doubleFromString\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                DoubleTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                
                // Handle requiredDouble with proper NaN detection
                double requiredDoubleValue = rs.getDouble("requiredDouble");
                Double actualRequiredDouble = rs.wasNull() ? null : requiredDoubleValue;
                
                // Handle optionalDouble
                Double actualOptionalDouble = rs.getObject("optionalDouble") != null ? rs.getDouble("optionalDouble") : null;

                // Handle additional optionals mapped to DOUBLE PRECISION
                Double actualOptionalByte = rs.getObject("optionalByte") != null ? rs.getDouble("optionalByte") : null;
                Double actualOptionalShort = rs.getObject("optionalShort") != null ? rs.getDouble("optionalShort") : null;
                Double actualOptionalInt = rs.getObject("optionalInt") != null ? rs.getDouble("optionalInt") : null;
                Double actualOptionalLong = rs.getObject("optionalLong") != null ? rs.getDouble("optionalLong") : null;
                Double actualOptionalReal = rs.getObject("optionalReal") != null ? rs.getDouble("optionalReal") : null;
                Double actualDoubleFromString = rs.getObject("doubleFromString") != null ? rs.getDouble("doubleFromString") : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification with tolerance for floating-point precision
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                
                // Use tolerance for double comparison to handle floating-point precision issues
                if (expected.getRequiredDouble() != null) {
                    Double expectedRequiredDoubleObj = expected.getRequiredDouble();
                    double expectedRequiredDouble = expectedRequiredDoubleObj.doubleValue();
                    assertNotNull(actualRequiredDouble, "RequiredDouble should not be null at index " + recordIndex);
                    double actualRequiredDoublePrimitive = actualRequiredDouble.doubleValue();
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expectedRequiredDouble) * 1e-15);
                    assertEquals(expectedRequiredDouble, actualRequiredDoublePrimitive, tolerance,
                        "RequiredDouble mismatch at index " + recordIndex + " ==> expected: <" + expectedRequiredDouble + "> but was: <" + actualRequiredDoublePrimitive + ">");
                }

                // Null handling verification for optional double
                if (expected.getOptionalDouble() == null) {
                    assertNull(actualOptionalDouble, 
                        "OptionalDouble should be null at index " + recordIndex);
                } else {
                    Double expectedOptionalDoubleObj = expected.getOptionalDouble();
                    double expectedOptionalDouble = expectedOptionalDoubleObj.doubleValue();
                    assertNotNull(actualOptionalDouble, "OptionalDouble should not be null at index " + recordIndex);
                    double actualOptionalDoublePrimitive = actualOptionalDouble.doubleValue();
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expectedOptionalDouble) * 1e-15);
                    assertEquals(expectedOptionalDouble, actualOptionalDoublePrimitive, tolerance,
                        "OptionalDouble mismatch at index " + recordIndex + " ==> expected: <" + expectedOptionalDouble + "> but was: <" + actualOptionalDoublePrimitive + ">");
                }

                // Validate additional optionals mapped to DOUBLE PRECISION
                if (expected.getOptionalByte() == null) {
                    assertNull(actualOptionalByte, "OptionalByte should be null at index " + recordIndex);
                } else {
                    double expectedValue = expected.getOptionalByte().doubleValue();
                    assertNotNull(actualOptionalByte, "OptionalByte should not be null at index " + recordIndex);
                    double tolerance = 0.0;
                    assertEquals(expectedValue, actualOptionalByte.doubleValue(), tolerance,
                        "OptionalByte mismatch at index " + recordIndex);
                }

                if (expected.getOptionalShort() == null) {
                    assertNull(actualOptionalShort, "OptionalShort should be null at index " + recordIndex);
                } else {
                    double expectedValue = expected.getOptionalShort().doubleValue();
                    assertNotNull(actualOptionalShort, "OptionalShort should not be null at index " + recordIndex);
                    double tolerance = 0.0;
                    assertEquals(expectedValue, actualOptionalShort.doubleValue(), tolerance,
                        "OptionalShort mismatch at index " + recordIndex);
                }

                if (expected.getOptionalInt() == null) {
                    assertNull(actualOptionalInt, "OptionalInt should be null at index " + recordIndex);
                } else {
                    double expectedValue = expected.getOptionalInt().doubleValue();
                    assertNotNull(actualOptionalInt, "OptionalInt should not be null at index " + recordIndex);
                    double tolerance = 0.0;
                    assertEquals(expectedValue, actualOptionalInt.doubleValue(), tolerance,
                        "OptionalInt mismatch at index " + recordIndex);
                }

                if (expected.getOptionalLong() == null) {
                    assertNull(actualOptionalLong, "OptionalLong should be null at index " + recordIndex);
                } else {
                    double expectedValue = expected.getOptionalLong().doubleValue();
                    assertNotNull(actualOptionalLong, "OptionalLong should not be null at index " + recordIndex);
                    double tolerance = 0.0;
                    assertEquals(expectedValue, actualOptionalLong.doubleValue(), tolerance,
                        "OptionalLong mismatch at index " + recordIndex);
                }

                if (expected.getOptionalReal() == null) {
                    assertNull(actualOptionalReal, "OptionalReal should be null at index " + recordIndex);
                } else {
                    double expectedValue = expected.getOptionalReal().doubleValue();
                    assertNotNull(actualOptionalReal, "OptionalReal should not be null at index " + recordIndex);
                    double tolerance = Math.max(1e-7, Math.abs(expectedValue) * 1e-7);
                    assertEquals(expectedValue, actualOptionalReal.doubleValue(), tolerance,
                        "OptionalReal mismatch at index " + recordIndex);
                }

                if (expected.getDoubleFromString() == null) {
                    assertNull(actualDoubleFromString, "DoubleFromString should be null at index " + recordIndex);
                } else {
                    double expectedValue = Double.parseDouble(expected.getDoubleFromString());
                    assertNotNull(actualDoubleFromString, "DoubleFromString should not be null at index " + recordIndex);
                    double tolerance = Math.max(1e-15, Math.abs(expectedValue) * 1e-15);
                    assertEquals(expectedValue, actualDoubleFromString.doubleValue(), tolerance,
                        "DoubleFromString mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyDoubleArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyDoubleArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                verifyDoubleArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                
                // Optional list with non-null elements verification
                verifyDoubleArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
    
    /**
     * Verifies a double array field using Array object instead of string parsing.
     */
    private void verifyDoubleArray(String fieldName, List<Double> expected, Array actualArray, 
                                  int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is DOUBLE (Types.DOUBLE = 8)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.DOUBLE, baseType,
            fieldName + " should have base type DOUBLE (8) at index " + recordIndex);

        // Get the array as Double array and convert to List
        Double[] arrayElements = (Double[]) actualArray.getArray();
        List<Double> actualList = Arrays.asList(arrayElements);

        // Direct list comparison with tolerance for floating-point precision
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            Double expectedElement = expected.get(i);
            Double actualElement = actualList.get(i);
            
            if (allowNullElements) {
                if (expectedElement == null) {
                    assertNull(actualElement, 
                        fieldName + " element " + i + " should be null at index " + recordIndex);
                } else {
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expectedElement) * 1e-15);
                    assertEquals(expectedElement, actualElement, tolerance,
                        fieldName + " element " + i + " mismatch at index " + recordIndex + " ==> expected: <" + expectedElement + "> but was: <" + actualElement + ">");
                }
            } else {
                // Calculate appropriate tolerance based on the magnitude of the expected value
                double tolerance = Math.max(1e-15, Math.abs(expectedElement) * 1e-15);
                assertEquals(expectedElement, actualElement, tolerance,
                    fieldName + " element " + i + " mismatch at index " + recordIndex + " ==> expected: <" + expectedElement + "> but was: <" + actualElement + ">");
            }
        }
    }
    
    /**
     * Helper method to create a large list with nullable elements.
     */
    private List<Double> createLargeListWithNulls(int size) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 10 == 0) {
                result.add(null);
            } else if (i % 100 == 1) {
                result.add(Double.MIN_VALUE);  // Include edge cases occasionally
            } else if (i % 100 == 2) {
                result.add(Double.MAX_VALUE);
            } else if (i % 1000 == 3) {
                result.add(Math.PI);
            } else if (i % 1000 == 4) {
                result.add(Math.E);
            } else if (i % 500 == 5) {
                result.add(Double.MIN_NORMAL);
            } else {
                result.add(i * 1.5);
            }
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null elements.
     */
    private List<Double> createLargeListWithoutNulls(int size) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 100 == 1) {
                result.add(Double.MIN_VALUE);
            } else if (i % 100 == 2) {
                result.add(Double.MAX_VALUE);
            } else if (i % 500 == 3) {
                result.add(Math.PI);
            } else if (i % 500 == 4) {
                result.add(Math.E);
            } else if (i % 250 == 5) {
                result.add(Double.MIN_NORMAL);
            } else if (i % 750 == 6) {
                result.add(-Double.MIN_NORMAL);
            } else {
                result.add(i * 10.5);
            }
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with negative values.
     */
    private List<Double> createOptionalLargeList(int size) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 200 == 1) {
                result.add(-Double.MIN_VALUE);
            } else if (i % 200 == 2) {
                result.add(-Double.MAX_VALUE);
            } else if (i % 800 == 3) {
                result.add(-Math.PI);
            } else if (i % 800 == 4) {
                result.add(-Math.E);
            } else if (i % 400 == 5) {
                result.add(-Double.MIN_NORMAL);
            } else {
                result.add(i * -1.5);
            }
        }
        return result;
    }

    private static DoubleTestRecord.DoubleTestRecordBuilder aValidTestRecord(int recordId) {
        return DoubleTestRecord.builder()
                .recordId(recordId)
                .requiredDouble(42.5)
                .optionalDouble(100.75)
                .optionalByte((byte) (recordId % 3))
                .optionalShort((short) (recordId % 5))
                .optionalInt(recordId * 10)
                .optionalLong((long) recordId * 100)
                .optionalReal(12.5f)
                .doubleFromString("123.45")
                .requiredListWithNullableElements(Arrays.asList(1.5, null, 3.25, null, 5.75))
                .requiredListWithNonNullElements(Arrays.asList(10.1, 20.2, 30.3, 40.4, 50.5))
                .optionalList(Arrays.asList(100.1, 200.2, 300.3))
                .optionalListWithNonNullElements(Arrays.asList(111.1, 222.2, 333.3));
    }

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(Map<String, String> connectorOverrides) throws Exception {
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                doubleTableSchema(), jsonDoubleSchema(), connectorOverrides);

        producer = initializeJsonProducer();

        DoubleTestRecord validRecord1 = aValidTestRecord(301)
                .doubleFromString("42.42")
                .build();
        DoubleTestRecord validRecord2 = aValidTestRecord(302)
                .doubleFromString("-17.5")
                .build();
        DoubleTestRecord invalidRecord1 = aValidTestRecord(303)
                .doubleFromString("abc")
                .build();
        DoubleTestRecord invalidRecord2 = aValidTestRecord(304)
                .doubleFromString("09-07-2025")
                .build();

        List<DoubleTestRecord> testRecords = List.of(
                validRecord1,
                invalidRecord1,
                validRecord2,
                invalidRecord2
        );

        publishMessages(testRecords);

        List<DoubleTestRecord> expectedRecords = List.of(validRecord1, validRecord2);
        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());

        verifyDoubleRecordsInFirebolt(expectedRecords);
    }

}