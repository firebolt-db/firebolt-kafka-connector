package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.NumericTestRecord;
 
import java.math.BigDecimal;
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
public class NumericSchemalessSerializerTest extends SchemalessBaseIntegrationTest {
    
    private static final String TABLE_NAME = generateTableName("numeric_test_table");
    private static final String TOPIC_NAME = generateTopicName("numeric-test-topic");
    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("numeric-serializer-test");

    }
    
    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }
        
        // Clean up test resources
        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);
        
        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("ingestionTypesWithOrWithoutNulls")
    void testNumericSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for numeric data type (schemaless)", testDescription);

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, numericTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer(includeNulls);
        
        List<NumericTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyNumericRecordsInFirebolt(testRecords);
    }

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(Map<String, String> connectorOverrides) throws Exception {
        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, numericTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer();

        NumericTestRecord validRecord1 = aValidTestRecord(301)
                .bigDecimalFromString("42.42")
                .build();
        NumericTestRecord validRecord2 = aValidTestRecord(302)
                .bigDecimalFromString("-17.5")
                .build();
        NumericTestRecord invalidRecord1 = aValidTestRecord(303)
                .bigDecimalFromString("abc")
                .build();
        NumericTestRecord invalidRecord2 = aValidTestRecord(304)
                .bigDecimalFromString("1,23")
                .build();

        List<NumericTestRecord> testRecords = List.of(
                validRecord1,
                invalidRecord1,
                validRecord2,
                invalidRecord2
        );

        publishMessages(testRecords);

        List<NumericTestRecord> expectedRecords = List.of(validRecord1, validRecord2);
        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());

        verifyNumericRecordsInFirebolt(expectedRecords);
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
            // use it as string as it loses precision when deserialized in Kafka Connect
            aValidTestRecord(2)
                .bigDecimalFromString("99999999999999999999999999999.123456789")// 29 digits before decimal
                .build(),

            aValidTestRecord(3)
                .bigDecimalFromString("-99999999999999999999999999999.987654321") // 29 digits before decimal
                .build(),

            // Record with minimum precision and scale
            aValidTestRecord(4)
                .requiredNumeric(new BigDecimal("0.000000001"))
                .optionalNumeric(new BigDecimal("-0.000000001"))
                .build(),

            // Record with null optional numeric
            aValidTestRecord(5)
                .optionalNumeric(null)
                .build(),

            // Record with zero values
            aValidTestRecord(6)
                .requiredNumeric(BigDecimal.ZERO)
                .optionalNumeric(BigDecimal.ZERO)
                .build(),

            // Record with large numbers (within NUMERIC(38,9) limits)
            aValidTestRecord(7)
                .bigDecimalFromString("12345678901234567890123456789.123456789") // 29 digits before decimal
                .build(),

            // Record with large numbers (within NUMERIC(38,9) limits)
            aValidTestRecord(8)
                .bigDecimalFromString("-98765432109876543210987654321.987654321") // 29 digits before decimal
                .build(),

            // Record with common decimal constants (truncated to 9 decimal places)
            aValidTestRecord(9)
                .requiredNumeric(new BigDecimal("3.141592653")) // Pi truncated to 9 decimal places
                .optionalNumeric(new BigDecimal("2.718281828")) // e truncated to 9 decimal places
                .build(),

            // we would have to create a list of strings as the big decimal is losing precision
            aValidTestRecord(10)
                .requiredListWithNullableElements(Arrays.asList(
                    new BigDecimal("898.123456789"),
                    null,
                    new BigDecimal("-999.987654321"),
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
            aValidTestRecord(11)
                .requiredListWithNullableElements(new ArrayList<>())
                .requiredListWithNonNullElements(new ArrayList<>())
                .optionalList(new ArrayList<>())
                .build(),

            // Record with null optional list
            aValidTestRecord(12)
                .optionalList(null)
                .optionalListWithNonNullElements(null)
                .build(),

            // Record with large lists
            aValidTestRecord(13)
                .requiredNumeric(new BigDecimal("42.123456789"))
                .optionalNumeric(new BigDecimal("-123.987654321"))
                .requiredListWithNullableElements(createLargeNumericListWithNulls(100))
                .requiredListWithNonNullElements(createLargeNumericListWithoutNulls(100))
                .optionalList(createOptionalLargeNumericList(100))
                .optionalListWithNonNullElements(createOptionalLargeNumericList(50))
                .build(),
                // Record with large numbers (within NUMERIC(38,9) limits)

            aValidTestRecord(14)
                    .bigDecimalFromString("1234.123456789012345678901234567890123") // 33 digits after the  decimal
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
                "\"optionalByte\" NUMERIC(38,9) NULL, " +
                "\"optionalShort\" NUMERIC(38,9) NULL, " +
                "\"optionalInt\" NUMERIC(38,9) NULL, " +
                "\"optionalLong\" NUMERIC(38,9) NULL, " +
                "\"optionalReal\" NUMERIC(38,9) NULL, " +
                "\"optionalDouble\" NUMERIC(38,9) NULL, " +
                "\"bigDecimalFromString\" NUMERIC(38,9) NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(NUMERIC(38,9) NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(NUMERIC(38,9) NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(NUMERIC(38,9) NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(NUMERIC(38,9) NOT NULL) NULL" +
                ")";
    }
    
    /**
     * Publishes NumericTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<NumericTestRecord> records) throws Exception {
        for (NumericTestRecord record : records) {
            String key = "numeric-test-key-" + record.getRecordId();
            ProducerRecord<String, String> producerRecord = 
                new ProducerRecord<>(TOPIC_NAME, key, mapper.writeValueAsString(record));

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
                .optionalByte((byte) (recordId % 3))
                .optionalShort((short) (recordId % 5))
                .optionalInt(recordId * 10)
                .optionalLong((long) recordId * 100)
                .optionalReal(12.5f)
                .optionalDouble(123.456)
                .bigDecimalFromString("123456.789123456")
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