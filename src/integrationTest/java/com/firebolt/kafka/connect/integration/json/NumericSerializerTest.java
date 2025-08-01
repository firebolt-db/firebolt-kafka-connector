package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.NumericTestRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import java.math.BigDecimal;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class NumericSerializerTest extends BaseIntegrationTest {
    
    private static final String TABLE_NAME = "numeric_test_table";
    private static final String TOPIC_NAME = "numeric-test-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, NumericTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("numeric-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         numericTableSchema(), jsonNumericSchema());
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
    void testNumericSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);
        
        List<NumericTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyNumericRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all numeric scenarios including edge cases.
     */
    private List<NumericTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with maximum precision and scale (38,9) - 29 digits before decimal, 9 after
            aValidTestRecord(2)
                .requiredNumeric(new BigDecimal("99999999999999999999999999999.123456789")) // 29 digits before decimal
                .optionalNumeric(new BigDecimal("-99999999999999999999999999999.987654321")) // 29 digits before decimal
                .build(),

            // Record with minimum precision and scale
            aValidTestRecord(3)
                .requiredNumeric(new BigDecimal("0.000000001"))
                .optionalNumeric(new BigDecimal("-0.000000001"))
                .build(),

            // Record with null optional numeric
            aValidTestRecord(4)
                .optionalNumeric(null)
                .build(),

            // Record with zero values
            aValidTestRecord(5)
                .requiredNumeric(BigDecimal.ZERO)
                .optionalNumeric(BigDecimal.ZERO)
                .build(),

            // Record with large numbers (within NUMERIC(38,9) limits)
            aValidTestRecord(6)
                .requiredNumeric(new BigDecimal("12345678901234567890123456789.123456789")) // 29 digits before decimal
                .optionalNumeric(new BigDecimal("-98765432109876543210987654321.987654321")) // 29 digits before decimal
                .build(),

            // Record with common decimal constants (truncated to 9 decimal places)
            aValidTestRecord(7)
                .requiredNumeric(new BigDecimal("3.141592653")) // Pi truncated to 9 decimal places
                .optionalNumeric(new BigDecimal("2.718281828")) // e truncated to 9 decimal places
                .build(),

            // Record with edge case lists (within NUMERIC(38,9) limits: 29 digits before decimal, 9 after)
            aValidTestRecord(8)
                .requiredListWithNullableElements(Arrays.asList(
                    new BigDecimal("99999999999999999999999999999.123456789"), // 29 digits before decimal
                    null,
                    new BigDecimal("-99999999999999999999999999999.987654321"), // 29 digits before decimal
                    new BigDecimal("0.000000001")
                ))
                .requiredListWithNonNullElements(Arrays.asList(
                    new BigDecimal("1.234567890"),
                    new BigDecimal("-2.345678901"),
                    new BigDecimal("3.456789012"),
                    new BigDecimal("4.567890123")
                ))
                .build(),

            // Record with empty lists
            aValidTestRecord(9)
                .requiredListWithNullableElements(new ArrayList<>())
                .requiredListWithNonNullElements(new ArrayList<>())
                .optionalList(new ArrayList<>())
                .build(),

            // Record with null optional list
            aValidTestRecord(10)
                .optionalList(null)
                .optionalListWithNonNullElements(null)
                .build(),

            // Record with large lists
            aValidTestRecord(11)
                .requiredNumeric(new BigDecimal("42.123456789"))
                .optionalNumeric(new BigDecimal("-123.987654321"))
                .requiredListWithNullableElements(createLargeNumericListWithNulls(100))
                .requiredListWithNonNullElements(createLargeNumericListWithoutNulls(100))
                .optionalList(createOptionalLargeNumericList(100))
                .optionalListWithNonNullElements(createOptionalLargeNumericList(50))
                .build()
        );
    }
    
    /**
     * Creates the Firebolt table with proper null/non-null constraints for numeric testing.
     */
    private Supplier<String> numericTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredNumeric\" NUMERIC(38,9) NOT NULL, " +
                "\"optionalNumeric\" NUMERIC(38,9) NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(NUMERIC(38,9) NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(NUMERIC(38,9) NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(NUMERIC(38,9) NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(NUMERIC(38,9) NOT NULL) NULL" +
                ")";
    }
    
    private Supplier<String> jsonNumericSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Numeric Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredNumeric\": {\n" +
                "      \"type\": \"string\",\n" +
                "      \"description\": \"Required numeric field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalNumeric\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional numeric field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"requiredListWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"string\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required list where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredListWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"string\"},\n" +
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
                "              {\"type\": \"string\"}\n" +
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
                "          \"items\": {\"type\": \"string\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional list where individual elements cannot be null\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredNumeric\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]\n" +
                "}";
    }
    
    /**
     * Publishes NumericTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<NumericTestRecord> records) throws Exception {
        for (NumericTestRecord record : records) {
            String key = "numeric-test-key-" + record.getRecordId();
            ProducerRecord<String, NumericTestRecord> producerRecord = 
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
     * Verifies that the published numeric records exist in the Firebolt table with correct null handling.
     */
    private void verifyNumericRecordsInFirebolt(List<NumericTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredNumeric\", \"optionalNumeric\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                NumericTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                BigDecimal actualRequiredNumeric = rs.getBigDecimal("requiredNumeric");
                BigDecimal actualOptionalNumeric = rs.getBigDecimal("optionalNumeric");
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");

                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId,
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(0, expected.getRequiredNumeric().compareTo(actualRequiredNumeric),
                    "RequiredNumeric mismatch at index " + recordIndex +
                    " (expected: " + expected.getRequiredNumeric() + ", actual: " + actualRequiredNumeric + ")");

                // Null handling verification for optional numeric
                if (expected.getOptionalNumeric() == null) {
                    assertNull(actualOptionalNumeric,
                        "OptionalNumeric should be null at index " + recordIndex);
                } else {
                    assertEquals(0, expected.getOptionalNumeric().compareTo(actualOptionalNumeric),
                        "OptionalNumeric mismatch at index " + recordIndex +
                        " (expected: " + expected.getOptionalNumeric() + ", actual: " + actualOptionalNumeric + ")");
                }

                // Array verification using getArray()
                verifyNumericArray("requiredListWithNullableElements",
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);

                verifyNumericArray("requiredListWithNonNullElements",
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);

                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray,
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyNumericArray("optionalList",
                        expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                }

                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray,
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyNumericArray("optionalListWithNonNullElements",
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                }
                
                log.debug("Verified numeric record {}: recordId={}, requiredNumeric={}", 
                    recordIndex, actualRecordId, actualRequiredNumeric);
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
    }
    
    /**
     * Verifies a numeric array field using Array object instead of string parsing.
     */
    private void verifyNumericArray(String fieldName, List<BigDecimal> expected, Array actualArray, 
                                  int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is NUMERIC (Types.NUMERIC = 2)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.NUMERIC, baseType,
            fieldName + " should have base type NUMERIC (2) at index " + recordIndex);

        // Get the array as BigDecimal array and convert to List<BigDecimal>
        BigDecimal[] arrayElements = (BigDecimal[]) actualArray.getArray();
        List<BigDecimal> actualList = Arrays.asList(arrayElements);

        // Direct list comparison with BigDecimal precision
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            BigDecimal expectedElement = expected.get(i);
            BigDecimal actualElement = actualList.get(i);
            
            if (expectedElement == null) {
                assertNull(actualElement, 
                    fieldName + " element " + i + " should be null at index " + recordIndex);
            } else {
                assertEquals(0, expectedElement.compareTo(actualElement),
                    fieldName + " element " + i + " mismatch at index " + recordIndex + 
                    " (expected: " + expectedElement + ", actual: " + actualElement + ")");
            }
        }
    }
    
    /**
     * Helper method to create a large list with nullable elements.
     */
    private List<BigDecimal> createLargeNumericListWithNulls(int size) {
        List<BigDecimal> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 5 == 0) {
                result.add(null);
            } else {
                result.add(new BigDecimal(i).add(new BigDecimal("0.123456789")));
            }
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null elements.
     */
    private List<BigDecimal> createLargeNumericListWithoutNulls(int size) {
        List<BigDecimal> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(new BigDecimal(i * 10).add(new BigDecimal("0.987654321")));
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with negative values.
     */
    private List<BigDecimal> createOptionalLargeNumericList(int size) {
        List<BigDecimal> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(new BigDecimal(i * -1).add(new BigDecimal("0.555555555")));
        }
        return result;
    }

    private NumericTestRecord.NumericTestRecordBuilder aValidTestRecord(int recordId) {
        return NumericTestRecord.builder()
                .recordId(recordId)
                .requiredNumeric(new BigDecimal("42.123456789"))
                .optionalNumeric(new BigDecimal("100.987654321"))
                .requiredListWithNullableElements(Arrays.asList(
                    new BigDecimal("1.111111111"), 
                    null, 
                    new BigDecimal("3.333333333"), 
                    null, 
                    new BigDecimal("5.555555555")))
                .requiredListWithNonNullElements(Arrays.asList(
                    new BigDecimal("10.123456789"), 
                    new BigDecimal("20.234567890"), 
                    new BigDecimal("30.345678901"), 
                    new BigDecimal("40.456789012"), 
                    new BigDecimal("50.567890123")))
                .optionalList(Arrays.asList(
                    new BigDecimal("100.111111111"), 
                    new BigDecimal("200.222222222"), 
                    new BigDecimal("300.333333333")))
                .optionalListWithNonNullElements(Arrays.asList(
                    new BigDecimal("111.444444444"), 
                    new BigDecimal("222.555555555"), 
                    new BigDecimal("333.666666666")));
    }
} 