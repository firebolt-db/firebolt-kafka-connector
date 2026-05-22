package com.firebolt.kafka.connect.integration.json.schema;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.IntegerTestRecord;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(TestTag.SERIALIZATION)
public class IntegerSchemaSerializerTest extends SchemaBaseIntegrationTest {
    
    private String TABLE_NAME = generateTableName("integer_test_table");
    private String TOPIC_NAME = generateTopicName("integer-test-topic");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, IntegerTestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("integer-serializer-test");
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
    void testIntegerSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for integer data type", testDescription);

        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                integerTableSchema(), jsonIntegerSchema(), connectorOverrides);

        producer = initializeJsonProducer(includeNulls);
        
        List<IntegerTestRecord> testRecords = createTestRecords();
        
        // publish the messages to kafka topic
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        // check that all the records have the expected value
        verifyIntegerRecordsInFirebolt(testRecords);
    }

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(Map<String, String> connectorOverrides) throws Exception {
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                integerTableSchema(), jsonIntegerSchema(), connectorOverrides);

        producer = initializeJsonProducer();

        IntegerTestRecord validRecord1 = aValidTestRecord(201)
                .build();
        IntegerTestRecord validRecord2 = aValidTestRecord(202)
                .build();
        IntegerTestRecord invalidRecord1 = aValidTestRecord(203)
                .integerFromString("abc")
                .build();
        IntegerTestRecord invalidRecord2 = aValidTestRecord(204)
                .integerFromString("1.23")
                .build();

        List<IntegerTestRecord> testRecords = List.of(
                validRecord1,
                invalidRecord1,
                validRecord2,
                invalidRecord2
        );

        publishMessages(testRecords);

        List<IntegerTestRecord> expectedRecords = List.of(validRecord1, validRecord2);
        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());

        verifyIntegerRecordsInFirebolt(expectedRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<IntegerTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with minimum value
            aValidTestRecord(2)
                .requiredInteger(Integer.MIN_VALUE)
                .build(),

            // Record with maximum value
            aValidTestRecord(3)
                .requiredInteger(Integer.MAX_VALUE)
                .build(),

            // Record with null integer
            aValidTestRecord(4)
                .optionalInteger(null)
                .build(),

            // required list with nullable (empty list)
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .build(),

            // required list but with null values
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(1, null, 2))
                .build(),

            // required list with min and max values
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(null, null, Integer.MAX_VALUE, Integer.MIN_VALUE))
                .build(),

            // required list with non-null values, but empty list
            aValidTestRecord(8)
                .requiredListWithNonNullElements(new ArrayList<>())
                .build(),

            // required list with non-null values and min and max values
            aValidTestRecord(9)
                .requiredListWithNonNullElements(Arrays.asList(Integer.MIN_VALUE, 123, -123, Integer.MAX_VALUE))
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
                .optionalList(Arrays.asList(-100, 200, null))
                .build(),

            // Record with valid optional list with null values, but empty array
            aValidTestRecord(13)
                .optionalListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with valid optional list with null values, but null
            aValidTestRecord(14)
                .optionalListWithNonNullElements(null)
                .build(),

            // Record with valid optional list without null values
            aValidTestRecord(15)
                .optionalListWithNonNullElements(Arrays.asList(-100, 0, 200))
                .build(),

            // Record with large lists (5000 elements each)
            aValidTestRecord(16)
                .requiredInteger(999)
                .optionalInteger(-999)
                .requiredListWithNullableElements(createLargeListWithNulls(5000))
                .requiredListWithNonNullElements(createLargeListWithoutNulls(5000))
                .optionalList(createOptionalLargeList(5000))
                .optionalListWithNonNullElements(createOptionalLargeList(3000))  // Different size for variety
                .build()
        );
    }
    
    private Supplier<String> integerTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredInteger\" INTEGER NOT NULL, " +
                "\"optionalInteger\" INTEGER NULL, " +
                "\"optionalShort\" INTEGER NULL, " +
                "\"optionalByte\" INTEGER NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(INTEGER NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(INTEGER NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(INTEGER NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(INTEGER NOT NULL) NULL, " +
                "\"integerFromString\" INTEGER NOT NULL" +
                ")";
    }
    
    private Supplier<String> jsonIntegerSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Integer Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\", \n " +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredInteger\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\", \n " +
                "      \"description\": \"Required integer field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalInteger\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int32\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional integer field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"optionalShort\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int16\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional short stored as INTEGER in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalByte\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int8\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional byte stored as INTEGER in Firebolt\"\n" +
                "    },\n" +
                "    \"requiredListWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"integer\", \"connect.type\": \"int32\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required list where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredListWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"integer\", \"connect.type\": \"int32\"},\n" +
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
                "              {\"type\": \"integer\", \"connect.type\": \"int32\"}\n" +
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
                "          \"items\": {\"type\": \"integer\", \"connect.type\": \"int32\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional list where individual elements cannot be null\"\n" +
                "    },\n" +
                "    \"integerFromString\": {\n" +
                "      \"type\": \"string\",\n" +
                "      \"description\": \"Integer value represented as string\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredInteger\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"integerFromString\"]\n" +
                "}";
    }
    
    private void publishMessages(List<IntegerTestRecord> records) throws Exception {
        for (IntegerTestRecord record : records) {
            String key = "integer-test-key-" + record.getRecordId();
            ProducerRecord<String, IntegerTestRecord> producerRecord = 
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
    
    private void verifyIntegerRecordsInFirebolt(List<IntegerTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredInteger\", \"optionalInteger\", " +
            "\"optionalShort\", \"optionalByte\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\", \"integerFromString\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                IntegerTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                Integer actualRequiredInteger = rs.getInt("requiredInteger");
                Integer actualOptionalInteger = rs.getObject("optionalInteger") != null ? rs.getInt("optionalInteger") : null;
                Integer actualOptionalShort = rs.getObject("optionalShort") != null ? rs.getInt("optionalShort") : null;
                Integer actualOptionalByte = rs.getObject("optionalByte") != null ? rs.getInt("optionalByte") : null;
                Integer actualIntegerFromString = rs.getInt("integerFromString");
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredInteger(), actualRequiredInteger, 
                    "RequiredInteger mismatch at index " + recordIndex);
                
                // Null handling verification for optional integer
                if (expected.getOptionalInteger() == null) {
                    assertNull(actualOptionalInteger, 
                        "OptionalInteger should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalInteger(), actualOptionalInteger, 
                        "OptionalInteger mismatch at index " + recordIndex);
                }
                if (expected.getOptionalShort() == null) {
                    assertNull(actualOptionalShort, 
                        "OptionalShort should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalShort().intValue(), actualOptionalShort, 
                        "OptionalShort mismatch at index " + recordIndex);
                }
                if (expected.getOptionalByte() == null) {
                    assertNull(actualOptionalByte, 
                        "OptionalByte should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalByte().intValue(), actualOptionalByte, 
                        "OptionalByte mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyIntegerArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyIntegerArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                verifyIntegerArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                
                // Optional list with non-null elements verification
                verifyIntegerArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);

                // Verify integerFromString column matches parsed value of string
                int expectedIntegerFromString = Integer.parseInt(expected.getIntegerFromString());
                assertEquals(expectedIntegerFromString, actualIntegerFromString,
                    "integerFromString mismatch at index " + recordIndex);
                
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
        assertEquals(expected, actualList,
            fieldName + " mismatch at index " + recordIndex);
    }
    
    /**
     * Helper method to create a large list with nullable elements.
     */
    private List<Integer> createLargeListWithNulls(int size) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : i);  // Every 5th element is null
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null elements.
     */
    private List<Integer> createLargeListWithoutNulls(int size) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i * 10);
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with negative values.
     */
    private List<Integer> createOptionalLargeList(int size) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(i * -1);
        }
        return result;
    }

    private IntegerTestRecord.IntegerTestRecordBuilder aValidTestRecord(int recordId) {
        return IntegerTestRecord.builder()
                .recordId(recordId)
                .requiredInteger(42)
                .optionalInteger(100)
                .optionalShort((short) (recordId % 2 == 0 ? 0 : 1))
                .optionalByte((byte) (recordId % 128))
                .requiredListWithNullableElements(Arrays.asList(1, null, 3, null, 5))
                .requiredListWithNonNullElements(Arrays.asList(10, 20, 30, 40, 50))
                .optionalList(Arrays.asList(100, 200, 300))
                .optionalListWithNonNullElements(Arrays.asList(111, 222, 333))
                .integerFromString(String.valueOf(recordId));
    }

}