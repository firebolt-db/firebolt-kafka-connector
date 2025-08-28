package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.DateTestRecord;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class DateSerializerTest extends BaseIntegrationTest {

    private static final String TOPIC_NAME = "date-test-topic";
    private static final String TABLE_NAME = "date_test_table";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, Object> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("date-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         dateTableSchema(), jsonDateSchema());
    }

    @AfterEach
    protected void tearDown() {
        // Clean up producer
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
    void testDateSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);
        
        List<DateTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyDateRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<DateTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with recent dates
            aValidTestRecord(2)
                .requiredDate(LocalDate.of(2024, 12, 31))
                .optionalDate(LocalDate.of(2025, 1, 1))
                .build(),

            // Record with historical dates
            aValidTestRecord(3)
                .requiredDate(LocalDate.of(1970, 1, 1))  // Unix epoch
                .optionalDate(LocalDate.of(2000, 1, 1))  // Y2K
                .build(),

            // Record with null optional date
            aValidTestRecord(4)
                .optionalDate(null)
                .build(),

            // Record with empty lists
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .requiredListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with nullable elements in list
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDate.of(2024, 1, 1), null, LocalDate.of(2024, 12, 31)))
                .build(),

            // Record with various date ranges
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(
                    null, LocalDate.of(1970, 1, 1), LocalDate.of(2024, 6, 15)))
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDate.of(2023, 1, 1), LocalDate.of(2024, 6, 15), LocalDate.of(2025, 12, 31)))
                .build(),

            // Record with null optional lists
            aValidTestRecord(8)
                .optionalList(null)
                .optionalListWithNonNullElements(null)
                .build(),

            // Record with empty optional lists
            aValidTestRecord(9)
                .optionalList(new ArrayList<>())
                .optionalListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with valid optional lists
            aValidTestRecord(10)
                .optionalList(Arrays.asList(LocalDate.of(2024, 3, 15), null, LocalDate.of(2024, 9, 30)))
                .optionalListWithNonNullElements(Arrays.asList(LocalDate.of(2024, 4, 1), LocalDate.of(2024, 8, 31)))
                .build(),

            // Record with leap year dates (February 29th)
            aValidTestRecord(11)
                .requiredDate(LocalDate.of(2024, 2, 29))  // Leap year date
                .optionalDate(LocalDate.of(2020, 2, 29))  // Another leap year date
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDate.of(2024, 2, 29), null, LocalDate.of(2020, 2, 29), null, LocalDate.of(2000, 2, 29)))
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDate.of(2024, 2, 29), LocalDate.of(2020, 2, 29), LocalDate.of(2016, 2, 29)))
                .optionalList(Arrays.asList(
                    null, LocalDate.of(2024, 2, 29), null, LocalDate.of(2012, 2, 29), null))
                .optionalListWithNonNullElements(Arrays.asList(
                    LocalDate.of(2008, 2, 29), LocalDate.of(2004, 2, 29)))
                .build(),

            // Record with large lists (100 elements each for performance testing)
            aValidTestRecord(12)
                .requiredDate(LocalDate.of(2024, 1, 1))
                .optionalDate(LocalDate.of(2024, 12, 31))
                .requiredListWithNullableElements(createLargeDateListWithNulls(100))
                .requiredListWithNonNullElements(createLargeDateListWithoutNulls(100))
                .optionalList(createOptionalLargeDateListWithNulls(100))  // Use version with nulls
                .optionalListWithNonNullElements(createOptionalLargeDateList(80))  // Use version without nulls
                .build()
        );
    }
    
    /**
     * Helper method to create a large list with nullable date elements.
     */
    private List<LocalDate> createLargeDateListWithNulls(int size) {
        List<LocalDate> result = new ArrayList<>();
        LocalDate baseDate = LocalDate.of(2024, 1, 1);
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : baseDate.plusDays(i));  // Every 5th element is null
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null date elements.
     */
    private List<LocalDate> createLargeDateListWithoutNulls(int size) {
        List<LocalDate> result = new ArrayList<>();
        LocalDate baseDate = LocalDate.of(2023, 1, 1);
        for (int i = 0; i < size; i++) {
            result.add(baseDate.plusDays(i * 2));  // Every other day
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with different date range.
     * Used for both optionalList and optionalListWithNonNullElements, so no nulls.
     */
    private List<LocalDate> createOptionalLargeDateList(int size) {
        List<LocalDate> result = new ArrayList<>();
        LocalDate baseDate = LocalDate.of(2025, 1, 1);
        for (int i = 0; i < size; i++) {
            result.add(baseDate.plusDays(i * 3));  // Every third day
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list WITH null values for testing nullable lists.
     */
    private List<LocalDate> createOptionalLargeDateListWithNulls(int size) {
        List<LocalDate> result = new ArrayList<>();
        LocalDate baseDate = LocalDate.of(2025, 1, 1);
        for (int i = 0; i < size; i++) {
            // Every 7th element is null to test null handling in optional lists
            if (i % 7 == 0) {
                result.add(null);
            } else {
                result.add(baseDate.plusDays(i * 3));  // Every third day
            }
        }
        return result;
    }

    private DateTestRecord.DateTestRecordBuilder aValidTestRecord(int recordId) {
        return DateTestRecord.builder()
                .recordId(recordId)
                .requiredDate(LocalDate.of(2024, 1, 15))
                .optionalDate(LocalDate.of(2024, 2, 28))
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDate.of(2024, 3, 1), null, LocalDate.of(2024, 3, 31), null, LocalDate.of(2024, 4, 15)))
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDate.of(2024, 5, 1), LocalDate.of(2024, 6, 15), LocalDate.of(2024, 7, 31)))
                .optionalList(Arrays.asList(
                    LocalDate.of(2024, 8, 1), LocalDate.of(2024, 9, 15), LocalDate.of(2024, 10, 31)))
                .optionalListWithNonNullElements(Arrays.asList(
                    LocalDate.of(2024, 11, 1), LocalDate.of(2024, 11, 15), LocalDate.of(2024, 12, 1)));
    }

    private Supplier<String> dateTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredDate\" DATE NOT NULL, " +
                "\"optionalDate\" DATE NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(DATE NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(DATE NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(DATE NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(DATE NOT NULL) NULL" +
                ")";
    }
    
    private Supplier<String> jsonDateSchema() {
        return () -> "{" +
                "\"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
                "\"title\": \"Date Test Record\"," +
                "\"type\": \"object\"," +
                "\"additionalProperties\": false," +
                "\"properties\": {" +
                    "\"recordId\": {" +
                        "\"type\": \"integer\"," +
                        "\"connect.type\": \"int32\",\n" +
                        "\"description\": \"Record identification number\"" +
                    "}," +
                    "\"requiredDate\": {" +
                        "\"type\": \"integer\"," +
                        "\"connect.type\": \"int32\"," +
                        "\"connect.version\": 1," +
                        "\"connect.name\": \"org.apache.kafka.connect.data.Date\"," +
                        "\"description\": \"Required date field\"" +
                    "}," +
                    "\"optionalDate\": {" +
                        "\"oneOf\": [" +
                            "{\"type\": \"null\"}," +
                            "{" +
                                "\"type\": \"integer\"," +
                                "\"connect.type\": \"int32\"," +
                                "\"connect.version\": 1," +
                                "\"connect.name\": \"org.apache.kafka.connect.data.Date\"" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional date field\"" +
                    "}," +
                    "\"requiredListWithNullableElements\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"oneOf\": [" +
                                "{\"type\": \"null\"}," +
                                "{" +
                                    "\"type\": \"integer\"," +
                                    "\"connect.type\": \"int32\"," +
                                    "\"connect.version\": 1," +
                                    "\"connect.name\": \"org.apache.kafka.connect.data.Date\"" +
                                "}" +
                            "]" +
                        "}," +
                        "\"description\": \"Required list with nullable elements\"" +
                    "}," +
                    "\"requiredListWithNonNullElements\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"type\": \"integer\"," +
                            "\"connect.type\": \"int32\"," +
                            "\"connect.version\": 1," +
                            "\"connect.name\": \"org.apache.kafka.connect.data.Date\"" +
                        "}," +
                        "\"description\": \"Required list with non-null elements\"" +
                    "}," +
                    "\"optionalList\": {" +
                        "\"oneOf\": [" +
                            "{\"type\": \"null\"}," +
                            "{" +
                                "\"type\": \"array\"," +
                                "\"items\": {" +
                                    "\"oneOf\": [" +
                                        "{\"type\": \"null\"}," +
                                        "{" +
                                            "\"type\": \"integer\"," +
                                            "\"connect.type\": \"int32\"," +
                                            "\"connect.version\": 1," +
                                            "\"connect.name\": \"org.apache.kafka.connect.data.Date\"" +
                                        "}" +
                                    "]" +
                                "}" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional list with nullable elements\"" +
                    "}," +
                    "\"optionalListWithNonNullElements\": {" +
                        "\"oneOf\": [" +
                            "{\"type\": \"null\"}," +
                            "{" +
                                "\"type\": \"array\"," +
                                "\"items\": {" +
                                    "\"type\": \"integer\"," +
                                    "\"connect.type\": \"int32\"," +
                                    "\"connect.version\": 1," +
                                    "\"connect.name\": \"org.apache.kafka.connect.data.Date\"" +
                                "}" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional list with non-null elements\"" +
                    "}" +
                "}," +
                "\"required\": [\"recordId\", \"requiredDate\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]" +
                "}";
    }
    
    /**
     * Publishes DateTestRecord messages to Kafka using JSON Schema serialization.
     * Converts LocalDate objects to integers (days since epoch) for Kafka Connect Date logical type.
     */
    private void publishMessages(List<DateTestRecord> records) throws Exception {
        for (DateTestRecord record : records) {
            String key = "date-test-key-" + record.getRecordId();
            
            // Convert LocalDate objects to integers (days since epoch) for Kafka Connect Date logical type
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("recordId", record.getRecordId());
            recordMap.put("requiredDate", localDateToEpochDays(record.getRequiredDate()));
            recordMap.put("optionalDate", record.getOptionalDate() != null ? localDateToEpochDays(record.getOptionalDate()) : null);
            
            // Convert date arrays
            recordMap.put("requiredListWithNullableElements", convertDateListToIntegerList(record.getRequiredListWithNullableElements()));
            recordMap.put("requiredListWithNonNullElements", convertDateListToIntegerList(record.getRequiredListWithNonNullElements()));
            recordMap.put("optionalList", record.getOptionalList() != null ? convertDateListToIntegerList(record.getOptionalList()) : null);
            recordMap.put("optionalListWithNonNullElements", record.getOptionalListWithNonNullElements() != null ? convertDateListToIntegerList(record.getOptionalListWithNonNullElements()) : null);
            
            ProducerRecord<String, Object> producerRecord = 
                new ProducerRecord<>(TOPIC_NAME, key, recordMap);
            
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
     * Converts LocalDate to days since Unix epoch (1970-01-01).
     */
    private int localDateToEpochDays(LocalDate date) {
        return Math.toIntExact(date.toEpochDay());
    }
    
    /**
     * Converts a list of LocalDate objects to a list of integers (days since epoch).
     */
    private List<Integer> convertDateListToIntegerList(List<LocalDate> dateList) {
        if (dateList == null) {
            return null;
        }
        return dateList.stream()
            .map(date -> date != null ? localDateToEpochDays(date) : null)
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Verifies that the published date records exist in the Firebolt table with correct null handling.
     */
    private void verifyDateRecordsInFirebolt(List<DateTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredDate\", \"optionalDate\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                DateTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                LocalDate actualRequiredDate = rs.getDate("requiredDate") != null ? 
                    rs.getDate("requiredDate").toLocalDate() : null;
                LocalDate actualOptionalDate = rs.getDate("optionalDate") != null ? 
                    rs.getDate("optionalDate").toLocalDate() : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredDate(), actualRequiredDate, 
                    "RequiredDate mismatch at index " + recordIndex);
                
                // Null handling verification for optional date
                if (expected.getOptionalDate() == null) {
                    assertNull(actualOptionalDate, 
                        "OptionalDate should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalDate(), actualOptionalDate, 
                        "OptionalDate mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyDateArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyDateArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                verifyDateArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                
                // Optional list with non-null elements verification
                verifyDateArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
    
    /**
     * Verifies a date array field using Array object instead of string parsing.
     */
    private void verifyDateArray(String fieldName, List<LocalDate> expected, Array actualArray, 
                               int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is DATE (Types.DATE = 91)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.DATE, baseType,
            fieldName + " should have base type DATE (91) at index " + recordIndex);

        // Get the array as Date array and convert to List<LocalDate>
        Date[] arrayElements = (Date[]) actualArray.getArray();
        List<LocalDate> actualList = new ArrayList<>();
        
        for (Date date : arrayElements) {
            if (date != null) {
                actualList.add(date.toLocalDate());
            } else {
                actualList.add(null);
            }
        }

        // Direct list comparison
        assertEquals(expected, actualList,
            fieldName + " mismatch at index " + recordIndex);
    }

}
