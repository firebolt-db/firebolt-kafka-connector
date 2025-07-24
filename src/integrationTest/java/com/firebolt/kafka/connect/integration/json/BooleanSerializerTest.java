package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.BooleanTestRecord;
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
public class BooleanSerializerTest extends BaseIntegrationTest {
    
    private static final String TABLE_NAME = "boolean_test_table";
    private static final String TOPIC_NAME = "boolean-test-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, BooleanTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("boolean-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         booleanTableSchema(), jsonBooleanSchema());
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
    void testBooleanSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);
        
        List<BooleanTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyBooleanRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<BooleanTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with false value
            aValidTestRecord(2)
                .requiredBoolean(false)
                .build(),

            // Record with true value
            aValidTestRecord(3)
                .requiredBoolean(true)
                .build(),

            // Record with null boolean
            aValidTestRecord(4)
                .optionalBoolean(null)
                .build(),

            // required list with nullable (empty list)
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .build(),

            // required list but with null values
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(true, null, false))
                .build(),

            // required list with mixed true/false and null values
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(null, null, true, false))
                .build(),

            // required list with non-null values, but empty list
            aValidTestRecord(8)
                .requiredListWithNonNullElements(new ArrayList<>())
                .build(),

            // required list with non-null values - all true
            aValidTestRecord(9)
                .requiredListWithNonNullElements(Arrays.asList(true, true, true, true))
                .build(),

            // Record with empty but valid optional list
            aValidTestRecord(10)
                .optionalList(new ArrayList<>())
                .build(),

            // Record with null optional list
            aValidTestRecord(11)
                .optionalList(null)
                .build(),

            // Record with valid optional list (includes nulls)
            aValidTestRecord(12)
                .optionalList(Arrays.asList(false, true, null))
                .build(),

            // Record with valid optional list with null values, but empty array
            aValidTestRecord(13)
                .optionalListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with valid optional list with null values, but null
            aValidTestRecord(14)
                .optionalListWithNonNullElements(null)
                .build(),

            // Record with valid optional list without null values - all false
            aValidTestRecord(15)
                .optionalListWithNonNullElements(Arrays.asList(false, false, false))
                .build(),

            // Record with large lists (5000 elements each)
            aValidTestRecord(16)
                .requiredBoolean(false)
                .optionalBoolean(true)
                .requiredListWithNullableElements(createLargeListWithNulls(5000))
                .requiredListWithNonNullElements(createLargeListWithoutNulls(5000))
                .optionalList(createOptionalLargeList(5000))
                .optionalListWithNonNullElements(createOptionalLargeList(3000))  // Different size for variety
                .build()
        );
    }
    
    private Supplier<String> booleanTableSchema() {
        return () -> "CREATE TABLE %s (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredBoolean\" BOOLEAN NOT NULL, " +
                "\"optionalBoolean\" BOOLEAN NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(BOOLEAN NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(BOOLEAN NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(BOOLEAN NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(BOOLEAN NOT NULL) NULL" +
                ")";
    }
    
    private Supplier<String> jsonBooleanSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Boolean Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredBoolean\": {\n" +
                "      \"type\": \"boolean\",\n" +
                "      \"description\": \"Required boolean field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalBoolean\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"boolean\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional boolean field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"requiredListWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"boolean\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required list where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredListWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"boolean\"},\n" +
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
                "              {\"type\": \"boolean\"}\n" +
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
                "          \"items\": {\"type\": \"boolean\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional list where individual elements cannot be null\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredBoolean\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]\n" +
                "}";
    }
    
    /**
     * Publishes BooleanTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<BooleanTestRecord> records) throws Exception {
        for (BooleanTestRecord record : records) {
            String key = "boolean-test-key-" + record.getRecordId();
            ProducerRecord<String, BooleanTestRecord> producerRecord = 
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
     * Verifies that the published boolean records exist in the Firebolt table with correct null handling.
     */
    private void verifyBooleanRecordsInFirebolt(List<BooleanTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);

        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredBoolean\", \"optionalBoolean\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\" " +
            "FROM %s ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                BooleanTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                Boolean actualRequiredBoolean = rs.getBoolean("requiredBoolean");
                Boolean actualOptionalBoolean = rs.getObject("optionalBoolean") != null ? rs.getBoolean("optionalBoolean") : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredBoolean(), actualRequiredBoolean, 
                    "RequiredBoolean mismatch at index " + recordIndex);
                
                // Null handling verification for optional boolean
                if (expected.getOptionalBoolean() == null) {
                    assertNull(actualOptionalBoolean, 
                        "OptionalBoolean should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalBoolean(), actualOptionalBoolean, 
                        "OptionalBoolean mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyBooleanArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyBooleanArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                verifyBooleanArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                
                // Optional list with non-null elements verification
                verifyBooleanArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
    
    /**
     * Verifies a boolean array field using Array object instead of string parsing.
     */
    private void verifyBooleanArray(String fieldName, List<Boolean> expected, Array actualArray, 
                                  int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is BOOLEAN (Types.BOOLEAN = 16)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.BOOLEAN, baseType,
            fieldName + " should have base type BOOLEAN (16) at index " + recordIndex);

        // Get the array as Boolean array and convert to List
        Boolean[] arrayElements = (Boolean[]) actualArray.getArray();
        List<Boolean> actualList = Arrays.asList(arrayElements);

        // Direct list comparison
        assertEquals(expected, actualList,
            fieldName + " mismatch at index " + recordIndex);
    }
    
    /**
     * Helper method to create a large list with nullable elements.
     */
    private List<Boolean> createLargeListWithNulls(int size) {
        List<Boolean> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 5 == 0) {
                result.add(null);  // Every 5th element is null
            } else {
                result.add(i % 2 == 0);  // Alternating true/false pattern
            }
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null elements.
     */
    private List<Boolean> createLargeListWithoutNulls(int size) {
        List<Boolean> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i % 3 == 0);  // Pattern: true, false, false, true, false, false...
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with different pattern.
     */
    private List<Boolean> createOptionalLargeList(int size) {
        List<Boolean> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i % 4 != 0);  // Pattern: false, true, true, true, false, true, true, true...
        }
        return result;
    }

    private BooleanTestRecord.BooleanTestRecordBuilder aValidTestRecord(int recordId) {
        return BooleanTestRecord.builder()
                .recordId(recordId)
                .requiredBoolean(true)
                .optionalBoolean(false)
                .requiredListWithNullableElements(Arrays.asList(true, null, false, null, true))
                .requiredListWithNonNullElements(Arrays.asList(false, true, false, true, false))
                .optionalList(Arrays.asList(true, false, true))
                .optionalListWithNonNullElements(Arrays.asList(false, false, true));
    }

} 