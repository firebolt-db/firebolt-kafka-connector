package com.firebolt.kafka.connect.integration.json.schema;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.BigIntTestRecord;
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
public class BigIntSchemaSerializerTest extends SchemaBaseIntegrationTest {
    
    private String TABLE_NAME = generateTableName("bigint_test_table");
    private String TOPIC_NAME = generateTopicName("bigint-test-topic");
    private String SCHEMA_SUBJECT = TOPIC_NAME + "-value";
    
    private Producer<String, BigIntTestRecord> producer;
    
    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("bigint-serializer-test");
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
    void testBigIntSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for big int data type", testDescription);

        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                bigIntTableSchema(), jsonBigIntSchema(), connectorOverrides);

        producer = initializeJsonProducer(includeNulls);
        
        // Create test records
        List<BigIntTestRecord> testRecords = createTestRecords();
        
        // Publish messages
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        // Verify records in Firebolt
        verifyBigIntRecordsInFirebolt(testRecords);
    }

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(Map<String,String> connectorOverrides) throws Exception {
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                bigIntTableSchema(), jsonBigIntSchema(), connectorOverrides);

        producer = initializeJsonProducer();

        BigIntTestRecord validRecord1 = aValidTestRecord(301)
                .stringBigInt("1234567890123")
                .build();
        BigIntTestRecord validRecord2 = aValidTestRecord(302)
                .stringBigInt("-999999999999")
                .build();
        BigIntTestRecord invalidRecord1 = aValidTestRecord(303)
                .stringBigInt("abc")
                .build();
        BigIntTestRecord invalidRecord2 = aValidTestRecord(304)
                .stringBigInt("9223372036854775808")
                .build();

        List<BigIntTestRecord> testRecords = List.of(
                validRecord1,
                invalidRecord1,
                validRecord2,
                invalidRecord2
        );

        publishMessages(testRecords);

        List<BigIntTestRecord> expectedRecords = List.of(validRecord1, validRecord2);
        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());

        verifyBigIntRecordsInFirebolt(expectedRecords);
    }
    
    private List<BigIntTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with minimum value
            aValidTestRecord(2)
                .requiredBigInt(Long.MIN_VALUE)
                .build(),

            // Record with maximum value
            aValidTestRecord(3)
                .requiredBigInt(Long.MAX_VALUE)
                .build(),

            // Record with null big integer
            aValidTestRecord(4)
                .optionalBigInt(null)
                .build(),

            // required list with nullable (empty list)
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .build(),

            // required list but with null values
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(1L, null, 2L))
                .build(),

            // required list with min and max values - the original problematic test case
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(null, null, Long.MAX_VALUE, Long.MIN_VALUE))
                .build(),

            // required list with non-null values, but empty list
            aValidTestRecord(8)
                .requiredListWithNonNullElements(new ArrayList<>())
                .build(),

            // required list with non-null values and min and max values
            aValidTestRecord(9)
                .requiredListWithNonNullElements(Arrays.asList(Long.MIN_VALUE, 123L, -123L, Long.MAX_VALUE))
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
                .optionalList(Arrays.asList(-100L, 200L, null))
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
                .optionalListWithNonNullElements(Arrays.asList(-100L, 0L, 200L))
                .build(),

            // Record with large lists (5000 elements each)
            aValidTestRecord(16)
                .requiredBigInt(999L)
                .optionalBigInt(-999L)
                .requiredListWithNullableElements(createLargeListWithNulls(5000))
                .requiredListWithNonNullElements(createLargeListWithoutNulls(5000))
                .optionalList(createOptionalLargeList(5000))
                .optionalListWithNonNullElements(createLargeListWithoutNulls(3000))  // Different size for variety
                .build()
        );
    }
    
    private Supplier<String> bigIntTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredBigInt\" BIGINT NOT NULL, " +
                "\"optionalBigInt\" BIGINT NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(BIGINT NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(BIGINT NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(BIGINT NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(BIGINT NOT NULL) NULL, " +
                "\"stringBigInt\" BIGINT NOT NULL, " +
                "\"optionalShort\" BIGINT NULL, " +
                "\"optionalInt\" BIGINT NULL, " +
                "\"optionalByte\" BIGINT NULL" +
                ")";
    }
    
    private Supplier<String> jsonBigIntSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"BigInt Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"recordId\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record identification number\"\n" +
                "    },\n" +
                "    \"requiredBigInt\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int64\",\n" +
                "      \"description\": \"Required big integer field - must not be null\"\n" +
                "    },\n" +
                "    \"optionalBigInt\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\", \"connect.type\": \"int64\"}\n" +
                "      ],\n" +
                "      \"description\": \"Optional big integer field - can be null or omitted\"\n" +
                "    },\n" +
                "    \"requiredListWithNullableElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"oneOf\": [\n" +
                "          {\"type\": \"null\"},\n" +
                "          {\"type\": \"integer\", \"connect.type\": \"int64\"}\n" +
                "        ]\n" +
                "      },\n" +
                "      \"description\": \"Required list where individual elements can be null\"\n" +
                "    },\n" +
                "    \"requiredListWithNonNullElements\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"integer\", \"connect.type\": \"int64\"},\n" +
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
                "              {\"type\": \"integer\", \"connect.type\": \"int64\"}\n" +
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
                "          \"items\": {\"type\": \"integer\", \"connect.type\": \"int64\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Optional list where individual elements cannot be null\"\n" +
                "    },\n" +
                "    \"stringBigInt\": {\n" +
                "      \"type\": \"string\",\n" +
                "      \"description\": \"BigInt value represented as string\"\n" +
                "    },\n" +
                "    \"optionalShort\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int16\",\n" +
                "      \"description\": \"Optional short stored as BIGINT in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalInt\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Optional int stored as BIGINT in Firebolt\"\n" +
                "    },\n" +
                "    \"optionalByte\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int8\",\n" +
                "      \"description\": \"Optional byte stored as BIGINT in Firebolt\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"recordId\", \"requiredBigInt\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"stringBigInt\"]\n" +
                "}";
    }
    
    private void publishMessages(List<BigIntTestRecord> records) throws Exception {
        log.info("Publishing {} BigInt records to topic: {}", records.size(), TOPIC_NAME);
        
        for (BigIntTestRecord record : records) {
            ProducerRecord<String, BigIntTestRecord> producerRecord = 
                new ProducerRecord<>(TOPIC_NAME, String.valueOf(record.getRecordId()), record);
            
            producer.send(producerRecord).get();
        }
    }
    
    private void verifyBigIntRecordsInFirebolt(List<BigIntTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredBigInt\", \"optionalBigInt\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", " +
            "\"optionalList\", \"optionalListWithNonNullElements\", \"stringBigInt\", \"optionalShort\", \"optionalInt\", \"optionalByte\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                BigIntTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                Long actualRequiredBigInt = rs.getLong("requiredBigInt");
                Long actualOptionalBigInt = rs.getObject("optionalBigInt", Long.class);
                Long actualStringBigInt = rs.getLong("stringBigInt");
                Long actualOptionalShort = rs.getObject("optionalShort", Long.class);
                Long actualOptionalInt = rs.getObject("optionalInt", Long.class);
                Long actualOptionalByte = rs.getObject("optionalByte", Long.class);
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredBigInt(), actualRequiredBigInt, 
                    "RequiredBigInt mismatch at index " + recordIndex);
                
                // Null handling verification for optional big int
                if (expected.getOptionalBigInt() == null) {
                    assertNull(actualOptionalBigInt, 
                        "OptionalBigInt should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalBigInt(), actualOptionalBigInt, 
                        "OptionalBigInt mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyBigIntArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex);
                    
                verifyBigIntArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex);
                
                verifyBigIntArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex);
                
                verifyBigIntArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex);

                // Verify stringBigInt parsed value
                long expectedStringBigInt = Long.parseLong(expected.getStringBigInt());
                assertEquals(expectedStringBigInt, actualStringBigInt,
                        "stringBigInt mismatch at index " + recordIndex);

                // Verify widened numeric optionals
                if (expected.getOptionalShort() == null) {
                    assertNull(actualOptionalShort, "optionalShort should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalShort().longValue(), actualOptionalShort);
                }
                if (expected.getOptionalInt() == null) {
                    assertNull(actualOptionalInt, "optionalInt should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalInt().longValue(), actualOptionalInt);
                }
                if (expected.getOptionalByte() == null) {
                    assertNull(actualOptionalByte, "optionalByte should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalByte().longValue(), actualOptionalByte);
                }
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
    
    private void verifyBigIntArray(String fieldName, List<Long> expected, Array actualArray, 
                                 int recordIndex) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is BIGINT (Types.BIGINT = -5)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.BIGINT, baseType,
            fieldName + " should have base type BIGINT (-5) at index " + recordIndex);

        // Get the array as Long array and convert to List<Long>
        Long[] arrayElements = (Long[]) actualArray.getArray();
        List<Long> actualList = Arrays.asList(arrayElements);

        // Direct list comparison
        assertEquals(expected, actualList,
            fieldName + " mismatch at index " + recordIndex);
    }
    
    private List<Long> createLargeListWithNulls(int size) {
        List<Long> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 3 == 0) {
                list.add(null);
            } else {
                list.add((long) i);
            }
        }
        return list;
    }
    
    private List<Long> createLargeListWithoutNulls(int size) {
        List<Long> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add((long) i);
        }
        return list;
    }
    
    private List<Long> createOptionalLargeList(int size) {
        List<Long> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 5 == 0) {
                list.add(null);
            } else {
                list.add((long) -i);
            }
        }
        return list;
    }
    
    private BigIntTestRecord.BigIntTestRecordBuilder aValidTestRecord(int recordId) {
        return BigIntTestRecord.builder()
            .recordId(recordId)
            .requiredBigInt(42L)
            .optionalBigInt(123L)
            .requiredListWithNullableElements(Arrays.asList(1L, 2L, 3L))
            .requiredListWithNonNullElements(Arrays.asList(10L, 20L, 30L))
            .optionalList(Arrays.asList(100L, 200L, 300L))
            .optionalListWithNonNullElements(Arrays.asList(1000L, 2000L, 3000L))
            .stringBigInt(String.valueOf(recordId))
            .optionalShort((short) (recordId % 2 == 0 ? 0 : 1))
            .optionalInt(recordId * 10)
            .optionalByte((byte) (recordId % 128));
    }
} 