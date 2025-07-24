package com.firebolt.kafka.connect.integration.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.StructTestRecord;
import com.firebolt.kafka.connect.integration.json.datatype.TestStruct;
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
public class StructSerializerTest extends BaseIntegrationTest {
    
    private static final String TABLE_NAME = "struct_test_table";
    private static final String TOPIC_NAME = "struct-test-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, StructTestRecord> producer;
    private ObjectMapper objectMapper;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("struct-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         structTableSchema(), jsonStructSchema());
        
        objectMapper = new ObjectMapper();
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
    void testStructSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);
        
        List<StructTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyStructRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<StructTestRecord> createTestRecords() {
        return Arrays.asList(
            // Minimal record with required fields only
            aValidTestRecord(1)
                .optionalStruct(null)
                .optionalStructArray(null)
                .optionalStructArrayWithNullableElements(null)
                .build(),

            // Comprehensive record with all fields
            aValidTestRecord(2)
                .build(),

            // Record with null optional fields
            aValidTestRecord(3)
                .optionalStruct(null)
                .optionalStructArray(null)
                .optionalStructArrayWithNullableElements(null)
                .build(),

            // Record with edge case values
            aValidTestRecord(4)
                .requiredStruct(createEdgeCaseTestStruct())
                .optionalStruct(createEdgeCaseTestStruct())
                .requiredStructArray(createEdgeCaseStructArray())
                .optionalStructArray(createEdgeCaseStructArray())
                .requiredStructArrayWithNullableElements(createEdgeCaseStructArrayWithNulls())
                .optionalStructArrayWithNullableElements(createEdgeCaseStructArrayWithNulls())
                .build(),

            // Record with large arrays
            aValidTestRecord(5)
                .requiredStructArray(createLargeStructArray())
                .optionalStructArray(createLargeStructArray())
                .requiredStructArrayWithNullableElements(createLargeStructArrayWithNulls())
                .optionalStructArrayWithNullableElements(createLargeStructArrayWithNulls())
                .build(),

            // Record with empty arrays
            aValidTestRecord(6)
                .requiredStructArray(Arrays.asList())
                .optionalStructArray(Arrays.asList())
                .requiredStructArrayWithNullableElements(Arrays.asList())
                .optionalStructArrayWithNullableElements(Arrays.asList())
                .build()
        );
    }

    /**
     * Creates the Firebolt table with proper null/non-null constraints for struct testing.
     */
    private Supplier<String> structTableSchema() {
        return () -> "CREATE TABLE %s (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredStruct\" TEXT NOT NULL, " +
                "\"optionalStruct\" TEXT NULL, " +
                "\"requiredStructArray\" TEXT NOT NULL, " +
                "\"optionalStructArray\" TEXT NULL, " +
                "\"requiredStructArrayWithNullableElements\" TEXT NOT NULL, " +
                "\"optionalStructArrayWithNullableElements\" TEXT NULL" +
                ")";
    }
    
    private Supplier<String> jsonStructSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Struct Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredStruct\": {\n" +
                "      \"type\": \"object\",\n" +
                "      \"properties\": {\n" +
                "        \"name\": {\n" +
                "          \"oneOf\": [\n" +
                "            {\"type\": \"null\"},\n" +
                "            {\"type\": \"string\"}\n" +
                "          ]\n" +
                "        },\n" +
                "        \"age\": {\n" +
                "          \"oneOf\": [\n" +
                "            {\"type\": \"null\"},\n" +
                "            {\"type\": \"integer\"}\n" +
                "          ]\n" +
                "        },\n" +
                "        \"active\": {\n" +
                "          \"oneOf\": [\n" +
                "            {\"type\": \"null\"},\n" +
                "            {\"type\": \"boolean\"}\n" +
                "          ]\n" +
                "        },\n" +
                "        \"score\": {\n" +
                "          \"oneOf\": [\n" +
                "            {\"type\": \"null\"},\n" +
                "            {\"type\": \"number\"}\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"required\": [\"name\", \"age\", \"active\", \"score\"],\n" +
                "      \"description\": \"Required struct field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalStruct\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"object\",\n" +
                "          \"properties\": {\n" +
                "            \"name\": {\n" +
                "              \"oneOf\": [\n" +
                "                {\"type\": \"null\"},\n" +
                "                {\"type\": \"string\"}\n" +
                "              ]\n" +
                "            },\n" +
                "            \"age\": {\n" +
                "              \"oneOf\": [\n" +
                "                {\"type\": \"null\"},\n" +
                "                {\"type\": \"integer\"}\n" +
                "              ]\n" +
                "            },\n" +
                "            \"active\": {\n" +
                "              \"oneOf\": [\n" +
                "                {\"type\": \"null\"},\n" +
                "                {\"type\": \"boolean\"}\n" +
                "              ]\n" +
                "            },\n" +
                "            \"score\": {\n" +
                "              \"oneOf\": [\n" +
                "                {\"type\": \"null\"},\n" +
                "                {\"type\": \"number\"}\n" +
                "              ]\n" +
                "            }\n" +
                "          },\n" +
                "          \"required\": [\"name\", \"age\", \"active\", \"score\"]\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional struct field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"requiredStructArray\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"type\": \"object\",\n" +
                "        \"properties\": {\n" +
                "          \"name\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"string\"}\n" +
                "            ]\n" +
                "          },\n" +
                "          \"age\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"integer\"}\n" +
                "            ]\n" +
                "          },\n" +
                "          \"active\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"boolean\"}\n" +
                "            ]\n" +
                "          },\n" +
                "          \"score\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"number\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        },\n" +
                "        \"required\": [\"name\", \"age\", \"active\", \"score\"]\n" +
                "      },\n" +
                "      \"description\": \"Required struct array - must not be null\"\n" +
                "    },\n" +
                "    \"optionalStructArray\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"type\": \"object\",\n" +
                "            \"properties\": {\n" +
                "              \"name\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"string\"}\n" +
                "                ]\n" +
                "              },\n" +
                "              \"age\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"integer\"}\n" +
                "                ]\n" +
                "              },\n" +
                "              \"active\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"boolean\"}\n" +
                "                ]\n" +
                "              },\n" +
                "              \"score\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"number\"}\n" +
                "                ]\n" +
                "              }\n" +
                "            },\n" +
                "            \"required\": [\"name\", \"age\", \"active\", \"score\"]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional struct array - can be null or omitted\"\n" +
                "    },\n" +
                "    \"requiredStructArrayWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\n" +
                "            \"type\": \"object\",\n" +
                "            \"properties\": {\n" +
                "              \"name\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"string\"}\n" +
                "                ]\n" +
                "              },\n" +
                "              \"age\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"integer\"}\n" +
                "                ]\n" +
                "              },\n" +
                "              \"active\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"boolean\"}\n" +
                "                ]\n" +
                "              },\n" +
                "              \"score\": {\n" +
                "                \"oneOf\": [\n" +
                "                  {\"type\": \"null\"},\n" +
                "                  {\"type\": \"number\"}\n" +
                "                ]\n" +
                "              }\n" +
                "            },\n" +
                "            \"required\": [\"name\", \"age\", \"active\", \"score\"]\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required struct array where individual elements can be null\"\n" +
                "    },\n" +
                "    \"optionalStructArrayWithNullableElements\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\n" +
                "                \"type\": \"object\",\n" +
                "                \"properties\": {\n" +
                "                  \"name\": {\n" +
                "                    \"oneOf\": [\n" +
                "                      {\"type\": \"null\"},\n" +
                "                      {\"type\": \"string\"}\n" +
                "                    ]\n" +
                "                  },\n" +
                "                  \"age\": {\n" +
                "                    \"oneOf\": [\n" +
                "                      {\"type\": \"null\"},\n" +
                "                      {\"type\": \"integer\"}\n" +
                "                    ]\n" +
                "                  },\n" +
                "                  \"active\": {\n" +
                "                    \"oneOf\": [\n" +
                "                      {\"type\": \"null\"},\n" +
                "                      {\"type\": \"boolean\"}\n" +
                "                    ]\n" +
                "                  },\n" +
                "                  \"score\": {\n" +
                "                    \"oneOf\": [\n" +
                "                      {\"type\": \"null\"},\n" +
                "                      {\"type\": \"number\"}\n" +
                "                    ]\n" +
                "                  }\n" +
                "                },\n" +
                "                \"required\": [\"name\", \"age\", \"active\", \"score\"]\n" +
                "              }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional struct array where individual elements can be null\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredStruct\", \"requiredStructArray\", \"requiredStructArrayWithNullableElements\"]\n" +
                "}";
    }

    /**
     * Publishes StructTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<StructTestRecord> records) throws Exception {
        for (StructTestRecord record : records) {
            String key = "struct-test-key-" + record.getRecordId();
            ProducerRecord<String, StructTestRecord> producerRecord = 
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
     * Verifies that the published struct records exist in the Firebolt table with correct null handling.
     */
    private void verifyStructRecordsInFirebolt(List<StructTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredStruct\", \"optionalStruct\", " +
            "\"requiredStructArray\", \"optionalStructArray\", \"requiredStructArrayWithNullableElements\", " +
            "\"optionalStructArrayWithNullableElements\" " +
            "FROM %s ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                StructTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                String actualRequiredStruct = rs.getString("requiredStruct");
                String actualOptionalStruct = rs.getString("optionalStruct");
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredStructArray = rs.getArray("requiredStructArray");
                Array actualOptionalStructArray = rs.getArray("optionalStructArray");
                Array actualRequiredStructArrayWithNullableElements = rs.getArray("requiredStructArrayWithNullableElements");
                Array actualOptionalStructArrayWithNullableElements = rs.getArray("optionalStructArrayWithNullableElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                
                // Struct verification
                verifyStruct("requiredStruct", expected.getRequiredStruct(), actualRequiredStruct, recordIndex);
                
                // Null handling verification for optional struct
                if (expected.getOptionalStruct() == null) {
                    assertNull(actualOptionalStruct, 
                        "OptionalStruct should be null at index " + recordIndex);
                } else {
                    verifyStruct("optionalStruct", expected.getOptionalStruct(), actualOptionalStruct, recordIndex);
                }
                
                // Array verification
                verifyStructArray("requiredStructArray", 
                    expected.getRequiredStructArray(), actualRequiredStructArray, recordIndex, false);
                    
                verifyStructArray("optionalStructArray", 
                    expected.getOptionalStructArray(), actualOptionalStructArray, recordIndex, true);
                
                verifyStructArray("requiredStructArrayWithNullableElements", 
                    expected.getRequiredStructArrayWithNullableElements(), actualRequiredStructArrayWithNullableElements, recordIndex, true);
                
                verifyStructArray("optionalStructArrayWithNullableElements", 
                    expected.getOptionalStructArrayWithNullableElements(), actualOptionalStructArrayWithNullableElements, recordIndex, true);
                
                log.debug("Verified struct record {}: recordId={}", recordIndex, actualRecordId);
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
    }
    
    /**
     * Verifies a struct field by comparing JSON strings.
     */
    private void verifyStruct(String fieldName, TestStruct expected, String actualJsonString, int recordIndex) {
        if (expected == null) {
            assertNull(actualJsonString, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        if (actualJsonString != null) {
            try {
                TestStruct actualStruct = objectMapper.readValue(actualJsonString, TestStruct.class);
                assertEquals(expected, actualStruct, fieldName + " mismatch at index " + recordIndex);
            } catch (Exception e) {
                log.error("Failed to parse {} JSON: {}", fieldName, actualJsonString, e);
                throw new RuntimeException("Failed to parse " + fieldName + " JSON", e);
            }
        } else {
            assertNull(expected, fieldName + " null mismatch at index " + recordIndex);
        }
    }
    
    /**
     * Verifies a struct array field using Array object instead of string parsing.
     */
    private void verifyStructArray(String fieldName, List<TestStruct> expected, Array actualArray, 
                                 int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is VARCHAR (Types.VARCHAR = 12) for JSON strings
        int baseType = actualArray.getBaseType();
        assertEquals(Types.VARCHAR, baseType,
            fieldName + " should have base type VARCHAR (12) at index " + recordIndex);

        // Get the array as String array and convert to List<TestStruct>
        String[] arrayElements = (String[]) actualArray.getArray();
        List<TestStruct> actualList = new ArrayList<>();
        
        for (String jsonString : arrayElements) {
            if (jsonString == null) {
                actualList.add(null);
            } else {
                try {
                    TestStruct struct = objectMapper.readValue(jsonString, TestStruct.class);
                    actualList.add(struct);
                } catch (Exception e) {
                    log.error("Failed to parse struct JSON: {}", jsonString, e);
                    throw new RuntimeException("Failed to parse struct JSON", e);
                }
            }
        }

        // Direct list comparison
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            TestStruct expectedElement = expected.get(i);
            TestStruct actualElement = actualList.get(i);
            
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
     * Helper method to create a valid test record using builder pattern with default values.
     * This provides a base record that can be customized for specific test scenarios.
     */
    private StructTestRecord.StructTestRecordBuilder aValidTestRecord(int recordId) {
        return StructTestRecord.builder()
                .recordId(recordId)
                .requiredStruct(createDefaultTestStruct())
                .optionalStruct(createDefaultTestStruct())
                .requiredStructArray(createDefaultStructArray())
                .optionalStructArray(createDefaultStructArray())
                .requiredStructArrayWithNullableElements(createDefaultStructArrayWithNulls())
                .optionalStructArrayWithNullableElements(createDefaultStructArrayWithNulls());
    }

    /**
     * Creates a default TestStruct with typical values.
     */
    private TestStruct createDefaultTestStruct() {
        return TestStruct.builder()
                .name("John Doe")
                .age(30)
                .active(true)
                .score(85.5)
                .build();
    }

    /**
     * Creates a TestStruct with edge case values.
     */
    private TestStruct createEdgeCaseTestStruct() {
        return TestStruct.builder()
                .name("") // Empty string
                .age(Integer.MAX_VALUE)
                .active(false)
                .score(Double.MAX_VALUE)
                .build();
    }

    /**
     * Creates a default array of TestStruct objects.
     */
    private List<TestStruct> createDefaultStructArray() {
        return Arrays.asList(
            TestStruct.builder().name("Alice").age(25).active(true).score(95.0).build(),
            TestStruct.builder().name("Bob").age(35).active(false).score(78.5).build(),
            TestStruct.builder().name("Charlie").age(28).active(true).score(88.0).build()
        );
    }

    /**
     * Creates an array of TestStruct objects with edge case values.
     */
    private List<TestStruct> createEdgeCaseStructArray() {
        return Arrays.asList(
            TestStruct.builder().name("").age(0).active(false).score(0.0).build(),
            TestStruct.builder().name("X").age(Integer.MIN_VALUE).active(true).score(Double.MIN_VALUE).build(),
            TestStruct.builder().name("Y").age(Integer.MAX_VALUE).active(false).score(Double.MAX_VALUE).build()
        );
    }

    /**
     * Creates a default array of TestStruct objects that includes null elements.
     */
    private List<TestStruct> createDefaultStructArrayWithNulls() {
        return Arrays.asList(
            TestStruct.builder().name("Valid1").age(20).active(true).score(90.0).build(),
            null, // Null element
            TestStruct.builder().name("Valid2").age(40).active(false).score(75.0).build(),
            null, // Another null element
            TestStruct.builder().name("Valid3").age(35).active(true).score(82.5).build()
        );
    }

    /**
     * Creates an array of TestStruct objects with edge cases and null elements.
     */
    private List<TestStruct> createEdgeCaseStructArrayWithNulls() {
        return Arrays.asList(
            null, // Start with null
            TestStruct.builder().name("").age(0).active(false).score(0.0).build(),
            null, // Middle null
            TestStruct.builder().name("Z").age(Integer.MAX_VALUE).active(true).score(Double.MAX_VALUE).build(),
            null  // End with null
        );
    }

    /**
     * Creates a large array of TestStruct objects for performance testing.
     */
    private List<TestStruct> createLargeStructArray() {
        List<TestStruct> largeArray = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeArray.add(TestStruct.builder()
                    .name("User" + i)
                    .age(20 + (i % 50))
                    .active(i % 2 == 0)
                    .score(50.0 + (i % 100))
                    .build());
        }
        return largeArray;
    }

    /**
     * Creates a large array of TestStruct objects with some null elements.
     */
    private List<TestStruct> createLargeStructArrayWithNulls() {
        List<TestStruct> largeArray = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            if (i % 5 == 0) {
                largeArray.add(null); // Every 5th element is null
            } else {
                largeArray.add(TestStruct.builder()
                        .name("LargeUser" + i)
                        .age(18 + (i % 60))
                        .active(i % 3 == 0)
                        .score(60.0 + (i % 80))
                        .build());
            }
        }
        return largeArray;
    }
} 