package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.ArrayTestRecord;
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
import org.junit.jupiter.api.Disabled;
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
public class ArraySerializerTest extends BaseIntegrationTest {

    private static final String TABLE_NAME = "array_test_table";
    private static final String TOPIC_NAME = "array-test-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, ArrayTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("array-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         arrayTableSchema(), jsonArraySchema());
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
    void testArraySerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);

        List<ArrayTestRecord> testRecords = createTestRecords();

        publishMessages(testRecords);

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());

        verifyArrayRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<ArrayTestRecord> createTestRecords() {
        return Arrays.asList(
                // Complete record with typical values
                aValidTestRecord(1)
                        .build(),

                // Record with empty arrays
                aValidTestRecord(2)
                        .requiredArrayWithNullableElements(new ArrayList<>())
                        .requiredArrayWithNonNullElements(new ArrayList<>())
                        .requiredArrayOfArraysWithNullableElements(new ArrayList<>())
                        .requiredArrayOfArraysWithNonNullElements(new ArrayList<>())
                        .build(),

                // Record with null optional arrays
                aValidTestRecord(3)
                        .optionalArray(null)
                        .optionalArrayWithNonNullElements(null)
                        .optionalArrayOfArrays(null)
                        .optionalArrayOfArraysWithNonNullElements(null)
                        .build(),

                // Record with arrays containing null elements
                aValidTestRecord(4)
                        .requiredArrayWithNullableElements(Arrays.asList(1, null, 3, null, 5))
                        .requiredArrayOfArraysWithNullableElements(Arrays.asList(
                                Arrays.asList(1, null, 3),
                                null,
                                Arrays.asList(4, 5, null),
                                Arrays.asList(null, 7, 8)
                        ))
                        .build(),

                // Record with arrays of arrays with null inner arrays
                aValidTestRecord(5)
                        .requiredArrayOfArraysWithNullableElements(Arrays.asList(
                                Arrays.asList(1, 2, 3),
                                null,
                                Arrays.asList(4, 5, 6),
                                null,
                                Arrays.asList(7, 8, 9)
                        ))
                        .build(),

                // Record with large arrays
                aValidTestRecord(6)
                        .requiredArrayWithNullableElements(createLargeArrayWithNulls(100))
                        .requiredArrayWithNonNullElements(createLargeArrayWithoutNulls(100))
                        .requiredArrayOfArraysWithNullableElements(createLargeArrayOfArraysWithNulls(10, 10))
                        .requiredArrayOfArraysWithNonNullElements(createLargeArrayOfArraysWithoutNulls(10, 10))
                        .build(),

                // Record with edge case arrays (min/max values)
                aValidTestRecord(7)
                        .requiredArrayWithNullableElements(Arrays.asList(Integer.MIN_VALUE, null, Integer.MAX_VALUE))
                        .requiredArrayWithNonNullElements(Arrays.asList(Integer.MIN_VALUE, 0, Integer.MAX_VALUE))
                        .requiredArrayOfArraysWithNullableElements(Arrays.asList(
                            Arrays.asList(Integer.MIN_VALUE, null, Integer.MAX_VALUE),
                            Arrays.asList(null, 0, null),
                            Arrays.asList(Integer.MAX_VALUE, null, Integer.MIN_VALUE)
                        ))
                        .requiredArrayOfArraysWithNonNullElements(Arrays.asList(
                            Arrays.asList(Integer.MIN_VALUE, 0, Integer.MAX_VALUE),
                            Arrays.asList(1, 2, 3),
                            Arrays.asList(Integer.MAX_VALUE, 0, Integer.MIN_VALUE)
                        ))
                        .build(),

                // Record with mixed array types
                aValidTestRecord(8)
                        .requiredArrayWithNullableElements(Arrays.asList(1, null, 3))
                        .requiredArrayWithNonNullElements(Arrays.asList(10, 20, 30))
                        .optionalArray(Arrays.asList(100, null, 300))
                        .optionalArrayWithNonNullElements(Arrays.asList(111, 222, 333))
                        .requiredArrayOfArraysWithNullableElements(Arrays.asList(
                                Arrays.asList(1, null, 3),
                                null,
                                Arrays.asList(4, 5, null)
                        ))
                        .requiredArrayOfArraysWithNonNullElements(Arrays.asList(
                                Arrays.asList(10, 20, 30),
                                Arrays.asList(40, 50, 60),
                                Arrays.asList(70, 80, 90)
                        ))
                        .optionalArrayOfArrays(Arrays.asList(
                                Arrays.asList(100, null, 300),
                                null,
                                Arrays.asList(400, 500, null)
                        ))
                        .optionalArrayOfArraysWithNonNullElements(Arrays.asList(
                                Arrays.asList(111, 222, 333),
                                Arrays.asList(444, 555, 666),
                                Arrays.asList(777, 888, 999)
                        ))
                        .build(),

                // Record with only required fields
                aValidTestRecord(9)
                        .optionalArray(null)
                        .optionalArrayWithNonNullElements(null)
                        .optionalArrayOfArrays(null)
                        .optionalArrayOfArraysWithNonNullElements(null)
                        .build(),

                // Record with complex nested arrays
                aValidTestRecord(10)
                        .requiredArrayOfArraysWithNullableElements(Arrays.asList(
                                Arrays.asList(1, 2, 3, 4, 5),
                                Arrays.asList(10, 20, 30),
                                Arrays.asList(100, 200),
                                Arrays.asList(1000)
                        ))
                        .requiredArrayOfArraysWithNonNullElements(Arrays.asList(
                                Arrays.asList(1, 2, 3, 4, 5),
                                Arrays.asList(10, 20, 30),
                                Arrays.asList(100, 200),
                                Arrays.asList(1000)
                        ))
                        .build()
        );
    }

    /**
     * Creates the Firebolt table with proper null/non-null constraints for array testing.
     */
    private Supplier<String> arrayTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredArrayWithNullableElements\" ARRAY(INTEGER NULL) NOT NULL, " +
                "\"requiredArrayWithNonNullElements\" ARRAY(INTEGER NOT NULL) NOT NULL, " +
                "\"optionalArray\" ARRAY(INTEGER NULL) NULL, " +
                "\"optionalArrayWithNonNullElements\" ARRAY(INTEGER NOT NULL) NULL, " +
                "\"requiredArrayOfArraysWithNullableElements\" ARRAY(ARRAY(INTEGER NULL) NULL) NOT NULL, " +
                "\"requiredArrayOfArraysWithNonNullElements\" ARRAY(ARRAY(INTEGER NOT NULL) NOT NULL) NOT NULL, " +
                "\"optionalArrayOfArrays\" ARRAY(ARRAY(INTEGER NULL) NULL) NULL, " +
                "\"optionalArrayOfArraysWithNonNullElements\" ARRAY(ARRAY(INTEGER NOT NULL) NOT NULL) NULL" +
                ")";
    }

    private Supplier<String> jsonArraySchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Array Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredArrayWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"integer\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required array where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredArrayWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"integer\"},\n" +
                "      \"description\": \"Required array where individual elements cannot be null\"\n" +
                "    },\n" +
                "    \"optionalArray\": {\n" +
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
                "      \"description\": \"Optional array - entire array can be null or omitted, and elements can be null\"\n" +
                "    },\n" +
                "    \"optionalArrayWithNonNullElements\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"integer\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional array where individual elements cannot be null\"\n" +
                "    },\n" +
                "    \"requiredArrayOfArraysWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\n" +
                "            \"type\": \"array\",\n" +
                "            \"items\": {\n" +
                "              \"oneOf\": [\n" +
                "                {\"type\": \"null\"},\n" +
                "                {\"type\": \"integer\"}\n" +
                "              ]\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required array of arrays where both outer and inner elements can be null\"\n" +
                "    },\n" +
                "    \"requiredArrayOfArraysWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"type\": \"array\",\n" +
                "        \"items\": {\"type\": \"integer\"}\n" +
                "      },\n" +
                "      \"description\": \"Required array of arrays where neither outer nor inner elements can be null\"\n" +
                "    },\n" +
                "    \"optionalArrayOfArrays\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\n" +
                "                \"type\": \"array\",\n" +
                "                \"items\": {\n" +
                "                  \"oneOf\": [\n" +
                "                    {\"type\": \"null\"},\n" +
                "                    {\"type\": \"integer\"}\n" +
                "                  ]\n" +
                "                }\n" +
                "              }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional array of arrays - entire array can be null or omitted, and both outer and inner elements can be null\"\n" +
                "    },\n" +
                "    \"optionalArrayOfArraysWithNonNullElements\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\n" +
                "                \"type\": \"array\",\n" +
                "                \"items\": {\"type\": \"integer\"}\n" +
                "              }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional array of arrays - entire array can be null or omitted, and both outer and inner elements can be null\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredArrayWithNullableElements\", \"requiredArrayWithNonNullElements\", \"requiredArrayOfArraysWithNullableElements\", \"requiredArrayOfArraysWithNonNullElements\"]\n" +
                "}";
    }

    /**
     * Publishes ArrayTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<ArrayTestRecord> records) throws Exception {
        for (ArrayTestRecord record : records) {
            String key = "array-test-key-" + record.getRecordId();
            ProducerRecord<String, ArrayTestRecord> producerRecord =
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
     * Verifies that the published array records exist in the Firebolt table with correct null handling.
     */
    private void verifyArrayRecordsInFirebolt(List<ArrayTestRecord> expectedRecords) throws SQLException {
        log.info("Verifying array records in Firebolt table: {}", TABLE_NAME);

        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount,
                "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
                "SELECT \"recordId\", \"requiredArrayWithNullableElements\", \"requiredArrayWithNonNullElements\", " +
                        "\"optionalArray\", \"optionalArrayWithNonNullElements\", \"requiredArrayOfArraysWithNullableElements\", " +
                        "\"requiredArrayOfArraysWithNonNullElements\", \"optionalArrayOfArrays\", \"optionalArrayOfArraysWithNonNullElements\" " +
                        "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                ArrayTestRecord expected = expectedRecords.get(recordIndex);

                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredArrayWithNullableArray = rs.getArray("requiredArrayWithNullableElements");
                Array actualRequiredArrayWithNonNullArray = rs.getArray("requiredArrayWithNonNullElements");
                Array actualOptionalArrayArray = rs.getArray("optionalArray");
                Array actualOptionalArrayWithNonNullArray = rs.getArray("optionalArrayWithNonNullElements");
                Array actualRequiredArrayOfArraysWithNullableArray = rs.getArray("requiredArrayOfArraysWithNullableElements");
                Array actualRequiredArrayOfArraysWithNonNullArray = rs.getArray("requiredArrayOfArraysWithNonNullElements");
                Array actualOptionalArrayOfArraysArray = rs.getArray("optionalArrayOfArrays");
                Array actualOptionalArrayOfArraysWithNonNullArray = rs.getArray("optionalArrayOfArraysWithNonNullElements");

                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId,
                        "RecordId mismatch at index " + recordIndex);

                // Array verification using getArray()
                verifyIntegerArray("requiredArrayWithNullableElements",
                        expected.getRequiredArrayWithNullableElements(), actualRequiredArrayWithNullableArray, recordIndex, true);

                verifyIntegerArray("requiredArrayWithNonNullElements",
                        expected.getRequiredArrayWithNonNullElements(), actualRequiredArrayWithNonNullArray, recordIndex, false);

                // Optional array verification
                if (expected.getOptionalArray() == null) {
                    assertNull(actualOptionalArrayArray,
                            "OptionalArray should be null at index " + recordIndex);
                } else {
                    verifyIntegerArray("optionalArray",
                            expected.getOptionalArray(), actualOptionalArrayArray, recordIndex, true);
                }

                if (expected.getOptionalArrayWithNonNullElements() == null) {
                    assertNull(actualOptionalArrayWithNonNullArray,
                            "OptionalArrayWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyIntegerArray("optionalArrayWithNonNullElements",
                            expected.getOptionalArrayWithNonNullElements(), actualOptionalArrayWithNonNullArray, recordIndex, false);
                }

                // Array of arrays verification
                verifyArrayOfArrays("requiredArrayOfArraysWithNullableElements",
                        expected.getRequiredArrayOfArraysWithNullableElements(), actualRequiredArrayOfArraysWithNullableArray, recordIndex, true);

                verifyArrayOfArrays("requiredArrayOfArraysWithNonNullElements",
                        expected.getRequiredArrayOfArraysWithNonNullElements(), actualRequiredArrayOfArraysWithNonNullArray, recordIndex, false);

                // Optional array of arrays verification
                if (expected.getOptionalArrayOfArrays() == null) {
                    assertNull(actualOptionalArrayOfArraysArray,
                            "OptionalArrayOfArrays should be null at index " + recordIndex);
                } else {
                    verifyArrayOfArrays("optionalArrayOfArrays",
                            expected.getOptionalArrayOfArrays(), actualOptionalArrayOfArraysArray, recordIndex, true);
                }

                if (expected.getOptionalArrayOfArraysWithNonNullElements() == null) {
                    assertNull(actualOptionalArrayOfArraysWithNonNullArray,
                            "OptionalArrayOfArraysWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyArrayOfArrays("optionalArrayOfArraysWithNonNullElements",
                            expected.getOptionalArrayOfArraysWithNonNullElements(), actualOptionalArrayOfArraysWithNonNullArray, recordIndex, false);
                }

                log.debug("Verified array record {}: recordId={}", recordIndex, actualRecordId);
                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    /**
     * Verifies an integer array field using Array object instead of string parsing.
     */
    private void verifyIntegerArray(String fieldName, List<Integer> expected, Array actualArray,
                                    int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is INTEGER (Types.INTEGER = 4)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.INTEGER, baseType,
            fieldName + " should have base type INTEGER (4) at index " + recordIndex);

        // Get the array as Integer array and convert to List<Integer>
        Integer[] arrayElements = (Integer[]) actualArray.getArray();
        List<Integer> actualList = Arrays.asList(arrayElements);

        // Direct list comparison
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            Integer expectedElement = expected.get(i);
            Integer actualElement = actualList.get(i);
            
            if (expectedElement == null) {
                assertNull(actualElement, 
                    fieldName + " element " + i + " should be null at index " + recordIndex);
            } else {
                assertEquals(expectedElement, actualElement,
                    fieldName + " element " + i + " mismatch at index " + recordIndex);
            }
        }
    }

    /**
     * Verifies an array of arrays field using Array object instead of string parsing.
     */
    private void verifyArrayOfArrays(String fieldName, List<List<Integer>> expected, Array actualArray,
                                     int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is ARRAY (Types.ARRAY = 2003)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.INTEGER, baseType,
            fieldName + " should have base type INTEGER (4) at index " + recordIndex);

        // Get the array as Array array and convert to List<List<Integer>>
        Integer[][] arrayElements = (Integer[][]) actualArray.getArray();
        List<List<Integer>> actualList = new ArrayList<>();
        
        for (Integer[] innerArray : arrayElements) {
            if (innerArray == null) {
                actualList.add(null);
            } else {
                actualList.add(Arrays.asList(innerArray));
            }
        }

        // Direct list comparison
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            List<Integer> expectedInnerArray = expected.get(i);
            List<Integer> actualInnerArray = actualList.get(i);
            
            if (expectedInnerArray == null) {
                assertNull(actualInnerArray, 
                    fieldName + " inner array " + i + " should be null at index " + recordIndex);
            } else {
                assertEquals(expectedInnerArray.size(), actualInnerArray.size(),
                    fieldName + " inner array " + i + " size mismatch at index " + recordIndex);
                
                for (int j = 0; j < expectedInnerArray.size(); j++) {
                    Integer expectedElement = expectedInnerArray.get(j);
                    Integer actualElement = actualInnerArray.get(j);
                    
                    if (expectedElement == null) {
                        assertNull(actualElement,
                            fieldName + " inner array " + i + " element " + j + " should be null at index " + recordIndex);
                    } else {
                        assertEquals(expectedElement, actualElement,
                            fieldName + " inner array " + i + " element " + j + " mismatch at index " + recordIndex);
                    }
                }
            }
        }
    }

    /**
     * Helper method to create a large array with nullable elements.
     */
    private List<Integer> createLargeArrayWithNulls(int size) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : i);  // Every 5th element is null
        }
        return result;
    }

    /**
     * Helper method to create a large array without null elements.
     */
    private List<Integer> createLargeArrayWithoutNulls(int size) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i * 10);
        }
        return result;
    }

    /**
     * Helper method to create a large array of arrays with nullable elements.
     */
    private List<List<Integer>> createLargeArrayOfArraysWithNulls(int outerSize, int innerSize) {
        List<List<Integer>> result = new ArrayList<>();
        for (int i = 0; i < outerSize; i++) {
            if (i % 3 == 0) {  // Every 3rd outer array is null
                result.add(null);
            } else {
                List<Integer> innerArray = new ArrayList<>();
                for (int j = 0; j < innerSize; j++) {
                    innerArray.add(j % 4 == 0 ? null : i * 100 + j);  // Every 4th inner element is null
                }
                result.add(innerArray);
            }
        }
        return result;
    }

    /**
     * Helper method to create a large array of arrays without null elements.
     */
    private List<List<Integer>> createLargeArrayOfArraysWithoutNulls(int outerSize, int innerSize) {
        List<List<Integer>> result = new ArrayList<>();
        for (int i = 0; i < outerSize; i++) {
            List<Integer> innerArray = new ArrayList<>();
            for (int j = 0; j < innerSize; j++) {
                innerArray.add(i * 100 + j);
            }
            result.add(innerArray);
        }
        return result;
    }

    private ArrayTestRecord.ArrayTestRecordBuilder aValidTestRecord(int recordId) {
        return ArrayTestRecord.builder()
                .recordId(recordId)
                .requiredArrayWithNullableElements(Arrays.asList(1, null, 3, null, 5))
                .requiredArrayWithNonNullElements(Arrays.asList(10, 20, 30, 40, 50))
                .optionalArray(Arrays.asList(100, null, 300))
                .optionalArrayWithNonNullElements(Arrays.asList(111, 222, 333))
                .requiredArrayOfArraysWithNullableElements(Arrays.asList(
                        Arrays.asList(1, null, 3),
                        null,
                        Arrays.asList(4, 5, null)
                ))
                .requiredArrayOfArraysWithNonNullElements(Arrays.asList(
                        Arrays.asList(10, 20, 30),
                        Arrays.asList(40, 50, 60),
                        Arrays.asList(70, 80, 90)
                ))
                .optionalArrayOfArrays(Arrays.asList(
                        Arrays.asList(100, null, 300),
                        null,
                        Arrays.asList(400, 500, null)
                ))
                .optionalArrayOfArraysWithNonNullElements(Arrays.asList(
                        Arrays.asList(111, 222, 333),
                        Arrays.asList(444, 555, 666),
                        Arrays.asList(777, 888, 999)
                ));
    }
}