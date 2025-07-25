package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.DoubleTestRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.NOT_IMPLEMENTED)
public class DoubleSerializerTest extends BaseIntegrationTest {
    
    private static final String TABLE_NAME = "double_test_table";
    private static final String TOPIC_NAME = "double-test-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, DoubleTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("double-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         doubleTableSchema(), jsonDoubleSchema());
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
    @CsvSource({
        "true,  'WITH null fields included in JSON as field: null'",
        "false, 'WITH null fields omitted from JSON entirely'"
    })
    void testDoubleSerialization(boolean includeNulls, String testDescription) throws Exception {
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

            // Record with large lists (5000 elements each)
            aValidTestRecord(11)
                .requiredDouble(999.999999999999)
                .optionalDouble(-999.999999999999)
                .requiredListWithNullableElements(createLargeListWithNulls(5000))
                .requiredListWithNonNullElements(createLargeListWithoutNulls(5000))
                .optionalList(createOptionalLargeList(5000))
                .optionalListWithNonNullElements(createOptionalLargeList(3000))  // Different size for variety
                .build()
        );
    }
    
    private Supplier<String> doubleTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredDouble\" DOUBLE PRECISION NOT NULL, " +
                "\"optionalDouble\" DOUBLE PRECISION NULL, " +
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
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredDouble\": {\n" +
                "      \"type\": \"number\",\n" +
                "      \"format\": \"double\",\n" +
                "      \"description\": \"Required double field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalDouble\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"format\": \"double\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional double field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"requiredListWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"number\", \"format\": \"double\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required list where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredListWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"number\", \"format\": \"double\"},\n" +
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
                "              {\"type\": \"number\", \"format\": \"double\"}\n" +
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
                "          \"items\": {\"type\": \"number\", \"format\": \"double\"}\n" +
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
                Double actualRequiredDouble = rs.getDouble("requiredDouble");
                Double actualOptionalDouble = rs.getObject("optionalDouble") != null ? rs.getDouble("optionalDouble") : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification with tolerance for floating-point precision
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                
                // Use tolerance for double comparison to handle floating-point precision issues
                if (expected.getRequiredDouble() != null && !expected.getRequiredDouble().isNaN()) {
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expected.getRequiredDouble()) * 1e-15);
                    assertEquals(expected.getRequiredDouble(), actualRequiredDouble, tolerance,
                        "RequiredDouble mismatch at index " + recordIndex + " ==> expected: <" + expected.getRequiredDouble() + "> but was: <" + actualRequiredDouble + ">");
                } else if (expected.getRequiredDouble() != null && expected.getRequiredDouble().isNaN()) {
                    assertTrue(actualRequiredDouble.isNaN(), 
                        "RequiredDouble should be NaN at index " + recordIndex);
                }
                
                // Null handling verification for optional double
                if (expected.getOptionalDouble() == null) {
                    assertNull(actualOptionalDouble, 
                        "OptionalDouble should be null at index " + recordIndex);
                } else if (!expected.getOptionalDouble().isNaN()) {
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expected.getOptionalDouble()) * 1e-15);
                    assertEquals(expected.getOptionalDouble(), actualOptionalDouble, tolerance,
                        "OptionalDouble mismatch at index " + recordIndex + " ==> expected: <" + expected.getOptionalDouble() + "> but was: <" + actualOptionalDouble + ">");
                } else {
                    assertTrue(actualOptionalDouble.isNaN(), 
                        "OptionalDouble should be NaN at index " + recordIndex);
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
                } else if (!expectedElement.isNaN()) {
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expectedElement) * 1e-15);
                    assertEquals(expectedElement, actualElement, tolerance,
                        fieldName + " element " + i + " mismatch at index " + recordIndex + " ==> expected: <" + expectedElement + "> but was: <" + actualElement + ">");
                } else {
                    assertTrue(actualElement.isNaN(),
                        fieldName + " element " + i + " should be NaN at index " + recordIndex);
                }
            } else {
                if (!expectedElement.isNaN()) {
                    // Calculate appropriate tolerance based on the magnitude of the expected value
                    double tolerance = Math.max(1e-15, Math.abs(expectedElement) * 1e-15);
                    assertEquals(expectedElement, actualElement, tolerance,
                        fieldName + " element " + i + " mismatch at index " + recordIndex + " ==> expected: <" + expectedElement + "> but was: <" + actualElement + ">");
                } else {
                    assertTrue(actualElement.isNaN(),
                        fieldName + " element " + i + " should be NaN at index " + recordIndex);
                }
            }
        }
    }
    
    /**
     * Helper method to create a large list with nullable elements.
     */
    private List<Double> createLargeListWithNulls(int size) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : i * 1.5);  // Every 5th element is null
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null elements.
     */
    private List<Double> createLargeListWithoutNulls(int size) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i * 10.5);
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with negative values.
     */
    private List<Double> createOptionalLargeList(int size) {
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i * -1.5);
        }
        return result;
    }

    private DoubleTestRecord.DoubleTestRecordBuilder aValidTestRecord(int recordId) {
        return DoubleTestRecord.builder()
                .recordId(recordId)
                .requiredDouble(42.5)
                .optionalDouble(100.75)
                .requiredListWithNullableElements(Arrays.asList(1.5, null, 3.25, null, 5.75))
                .requiredListWithNonNullElements(Arrays.asList(10.1, 20.2, 30.3, 40.4, 50.5))
                .optionalList(Arrays.asList(100.1, 200.2, 300.3))
                .optionalListWithNonNullElements(Arrays.asList(111.1, 222.2, 333.3));
    }

} 