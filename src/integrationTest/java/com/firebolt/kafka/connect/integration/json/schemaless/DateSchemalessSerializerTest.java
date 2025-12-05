package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.DateTestRecord;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
public class DateSchemalessSerializerTest extends SchemalessBaseIntegrationTest {
    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private static final String TOPIC_NAME = generateTopicName("date-test-topic");
    private static final String TABLE_NAME = generateTableName("date_test_table");
    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("date-serializer-test");

    }

    @AfterEach
    protected void tearDown() {
        // Clean up producer
        if (producer != null) {
            producer.close();
        }
        
        // Clean up test resources
        cleanupSchemalessTestResources(TABLE_NAME, TOPIC_NAME);
        
        super.tearDown();
    }

    @ParameterizedTest
    @MethodSource("ingestionTypesWithOrWithoutNulls")
    void testDateSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for date data type (schemaless)", testDescription);

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, dateTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer(includeNulls);
        
        List<DateTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyDateRecordsInFirebolt(testRecords);
    }

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(Map<String, String> connectorOverrides) throws Exception {
        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, dateTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer();

        DateTestRecord validRecord1 = aValidTestRecord(401)
                .dateAsString("2024-01-15")
                .build();
        DateTestRecord validRecord2 = aValidTestRecord(402)
                .dateAsString("2025-12-31")
                .build();
        DateTestRecord invalidRecord1 = aValidTestRecord(403)
                .dateAsString("abc")
                .build();
        DateTestRecord invalidRecord2 = aValidTestRecord(404)
                .dateAsString("09-07-2025")
                .build();

        List<DateTestRecord> testRecords = List.of(
                validRecord1,
                invalidRecord1,
                validRecord2,
                invalidRecord2
        );

        publishMessages(testRecords);

        List<DateTestRecord> expectedRecords = List.of(validRecord1, validRecord2);
        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());

        verifyDateRecordsInFirebolt(expectedRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<DateTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .optionalLocalDate(LocalDate.of(2024, 3, 1))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 2)))
                .localDateIso8601(LocalDate.of(2024, 1, 10))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2024, 1, 10), LocalDate.of(2024, 1, 11)))
                .dateAsString("2024-01-15")
                .dateAsIso8601List(Arrays.asList("2024-01-10", "2024-01-11"))
                .build(),

            // Record with recent dates
            aValidTestRecord(2)
                .requiredDate(createDate(2024, Calendar.DECEMBER, 31))
                .optionalDate(createDate(2025, Calendar.JANUARY, 1))
                .optionalLocalDate(LocalDate.of(2025, 2, 1))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2025, 2, 1), LocalDate.of(2025, 2, 2)))
                .localDateIso8601(LocalDate.of(2025, 2, 15))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2025, 2, 15), LocalDate.of(2025, 2, 16)))
                .dateAsString("2025-02-01")
                .dateAsIso8601List(Arrays.asList("2025-02-15", "2025-02-16"))
                .build(),

            // Record with historical dates
            aValidTestRecord(3)
                .requiredDate(createDate(1970, Calendar.JANUARY, 1))  // Unix epoch
                .optionalDate(createDate(2000, Calendar.JANUARY, 1))  // Y2K
                .optionalLocalDate(LocalDate.of(2000, 1, 1))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(1970, 1, 1), LocalDate.of(2000, 1, 1)))
                .localDateIso8601(LocalDate.of(2000, 1, 2))
                .localDateIso8601List(Arrays.asList(LocalDate.of(1970, 1, 1), LocalDate.of(2000, 1, 2)))
                .dateAsString("2000-01-01")
                .dateAsIso8601List(Arrays.asList("1970-01-01", "2000-01-02"))
                .build(),

            // Record with null optional date
            aValidTestRecord(4)
                .optionalDate(null)
                .optionalLocalDate(null)
                .optionalLocalDateList(null)
                .localDateIso8601(null)
                .localDateIso8601List(null)
                .dateAsString(null)
                .dateAsIso8601List(null)
                .build(),

            // Record with empty lists
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .requiredListWithNonNullElements(new ArrayList<>())
                .optionalLocalDate(LocalDate.of(2024, 6, 1))
                .optionalLocalDateList(new ArrayList<>())
                .localDateIso8601(LocalDate.of(2024, 6, 2))
                .localDateIso8601List(new ArrayList<>())
                .dateAsString("2024-06-01")
                .dateAsIso8601List(new ArrayList<>())
                .build(),

            // Record with nullable elements in list
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(
                    createDate(2024, Calendar.JANUARY, 1), null, createDate(2024, Calendar.DECEMBER, 31)))
                .optionalLocalDate(LocalDate.of(2024, 12, 31))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2024, 12, 30), null, LocalDate.of(2024, 12, 31)))
                .localDateIso8601(LocalDate.of(2024, 12, 25))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2024, 12, 25), LocalDate.of(2024, 12, 31)))
                .dateAsString("2024-12-31")
                .dateAsIso8601List(Arrays.asList("2024-12-25", "2024-12-31"))
                .build(),

            // Record with various date ranges
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(
                    null, createDate(1970, Calendar.JANUARY, 1), createDate(2024, Calendar.JUNE, 15)))
                .requiredListWithNonNullElements(Arrays.asList(
                    createDate(2023, Calendar.JANUARY, 1), createDate(2024, Calendar.JUNE, 15), createDate(2025, Calendar.DECEMBER, 31)))
                .optionalLocalDate(LocalDate.of(2023, 1, 1))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2023, 1, 1), LocalDate.of(2024, 6, 15), LocalDate.of(2025, 12, 31)))
                .localDateIso8601(LocalDate.of(2023, 2, 1))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2023, 2, 1), LocalDate.of(2024, 6, 15), LocalDate.of(2025, 12, 31)))
                .dateAsString("2023-01-01")
                .dateAsIso8601List(Arrays.asList("2023-02-01", "2024-06-15", "2025-12-31"))
                .build(),

            // Record with null optional lists
            aValidTestRecord(8)
                .optionalList(null)
                .optionalListWithNonNullElements(null)
                .optionalLocalDate(LocalDate.of(2024, 7, 1))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2024, 7, 1)))
                .localDateIso8601(LocalDate.of(2024, 7, 2))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2024, 7, 2)))
                .dateAsString("2024-07-01")
                .dateAsIso8601List(Arrays.asList("2024-07-02"))
                .build(),

            // Record with empty optional lists
            aValidTestRecord(9)
                .optionalList(new ArrayList<>())
                .optionalListWithNonNullElements(new ArrayList<>())
                .optionalLocalDate(LocalDate.of(2024, 8, 1))
                .optionalLocalDateList(new ArrayList<>())
                .localDateIso8601(LocalDate.of(2024, 8, 2))
                .localDateIso8601List(new ArrayList<>())
                .dateAsString("2024-08-01")
                .dateAsIso8601List(new ArrayList<>())
                .build(),

            // Record with valid optional lists
            aValidTestRecord(10)
                .optionalList(Arrays.asList(createDate(2024, Calendar.MARCH, 15), null, createDate(2024, Calendar.SEPTEMBER, 30)))
                .optionalListWithNonNullElements(Arrays.asList(createDate(2024, Calendar.APRIL, 1), createDate(2024, Calendar.AUGUST, 31)))
                .optionalLocalDate(LocalDate.of(2024, 9, 1))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2024, 4, 1), LocalDate.of(2024, 8, 31)))
                .localDateIso8601(LocalDate.of(2024, 9, 2))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2024, 4, 2), LocalDate.of(2024, 8, 31)))
                .dateAsString("2024-09-01")
                .dateAsIso8601List(Arrays.asList("2024-04-02", "2024-08-31"))
                .build(),

            // Record with leap year dates (February 29th)
            aValidTestRecord(11)
                .requiredDate(createDate(2024, Calendar.FEBRUARY, 29))  // Leap year date
                .optionalDate(createDate(2020, Calendar.FEBRUARY, 29))  // Another leap year date
                .requiredListWithNullableElements(Arrays.asList(
                    createDate(2024, Calendar.FEBRUARY, 29), null, createDate(2020, Calendar.FEBRUARY, 29), null, createDate(2000, Calendar.FEBRUARY, 29)))
                .requiredListWithNonNullElements(Arrays.asList(
                    createDate(2024, Calendar.FEBRUARY, 29), createDate(2020, Calendar.FEBRUARY, 29), createDate(2016, Calendar.FEBRUARY, 29)))
                .optionalList(Arrays.asList(
                    null, createDate(2024, Calendar.FEBRUARY, 29), null, createDate(2012, Calendar.FEBRUARY, 29), null))
                .optionalListWithNonNullElements(Arrays.asList(
                    createDate(2008, Calendar.FEBRUARY, 29), createDate(2004, Calendar.FEBRUARY, 29)))
                .optionalLocalDate(LocalDate.of(2024, 2, 29))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2024, 2, 29), LocalDate.of(2020, 2, 29)))
                .localDateIso8601(LocalDate.of(2024, 2, 28))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2024, 2, 28), LocalDate.of(2020, 2, 29)))
                .dateAsString("2024-02-29")
                .dateAsIso8601List(Arrays.asList("2024-02-28", "2020-02-29"))
                .build(),

            // Record with large lists (100 elements each for performance testing)
            aValidTestRecord(12)
                .requiredDate(createDate(2024, Calendar.JANUARY, 1))
                .optionalDate(createDate(2024, Calendar.DECEMBER, 31))
                .requiredListWithNullableElements(createLargeDateListWithNulls(100))
                .requiredListWithNonNullElements(createLargeDateListWithoutNulls(100))
                .optionalList(createOptionalLargeDateListWithNulls(100))  // Use version with nulls
                .optionalListWithNonNullElements(createOptionalLargeDateList(80))  // Use version without nulls
                .optionalLocalDate(LocalDate.of(2024, 12, 31))
                .optionalLocalDateList(Arrays.asList(LocalDate.of(2024, 1, 1), LocalDate.of(2024, 12, 31)))
                .localDateIso8601(LocalDate.of(2024, 12, 30))
                .localDateIso8601List(Arrays.asList(LocalDate.of(2024, 1, 2), LocalDate.of(2024, 12, 30)))
                .dateAsString("2024-12-31")
                .dateAsIso8601List(Arrays.asList("2024-01-02", "2024-12-30"))
                .build()
        );
    }
    
    /**
     * Helper method to create a large list with nullable date elements.
     */
    private List<java.util.Date> createLargeDateListWithNulls(int size) {
        List<java.util.Date> result = new ArrayList<>();
        java.util.Date baseDate = createDate(2024, Calendar.JANUARY, 1);
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : baseDate);
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null date elements.
     */
    private List<java.util.Date> createLargeDateListWithoutNulls(int size) {
        List<java.util.Date> result = new ArrayList<>();
        java.util.Date baseDate = createDate(2023, Calendar.JANUARY, 1);
        for (int i = 0; i < size; i++) {
            result.add(baseDate);
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with different date range.
     * Used for both optionalList and optionalListWithNonNullElements, so no nulls.
     */
    private List<java.util.Date> createOptionalLargeDateList(int size) {
        List<java.util.Date> result = new ArrayList<>();
        java.util.Date baseDate = createDate(2025, Calendar.JANUARY, 1);
        for (int i = 0; i < size; i++) {
            result.add(baseDate);
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list WITH null values for testing nullable lists.
     */
    private List<java.util.Date> createOptionalLargeDateListWithNulls(int size) {
        List<java.util.Date> result = new ArrayList<>();
        java.util.Date baseDate = createDate(2025, Calendar.JANUARY, 1);
        for (int i = 0; i < size; i++) {
            if (i % 7 == 0) {
                result.add(null);
            } else {
                result.add(baseDate);
            }
        }
        return result;
    }

    private DateTestRecord.DateTestRecordBuilder aValidTestRecord(int recordId) {
        Calendar calRequired = Calendar.getInstance();
        calRequired.set(2024, Calendar.JANUARY, 15, 5, 0, 0);
        calRequired.set(Calendar.MILLISECOND, 0);

        Calendar calOptional = Calendar.getInstance();
        calOptional.set(2024, Calendar.FEBRUARY, 28, 5, 0, 0);
        calOptional.set(Calendar.MILLISECOND, 0);

        return DateTestRecord.builder()
                .recordId(recordId)
                .requiredDate(calRequired.getTime())
                .optionalDate(calOptional.getTime())
                .requiredListWithNullableElements(Arrays.asList(
                    createDate(2024, Calendar.MARCH, 1), null, createDate(2024, Calendar.MARCH, 31), null, createDate(2024, Calendar.APRIL, 15)))
                .requiredListWithNonNullElements(Arrays.asList(
                    createDate(2024, Calendar.MAY, 1), createDate(2024, Calendar.JUNE, 15), createDate(2024, Calendar.JULY, 31)))
                .optionalList(Arrays.asList(
                    createDate(2024, Calendar.AUGUST, 1), createDate(2024, Calendar.SEPTEMBER, 15), createDate(2024, Calendar.OCTOBER, 31)))
                .optionalListWithNonNullElements(Arrays.asList(
                    createDate(2024, Calendar.NOVEMBER, 1), createDate(2024, Calendar.NOVEMBER, 15), createDate(2024, Calendar.DECEMBER, 1)));
    }

    private java.util.Date createDate(int year, int monthConstant, int dayOfMonth) {
        Calendar c = Calendar.getInstance();
        c.set(year, monthConstant, dayOfMonth, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    private Supplier<String> dateTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredDate\" DATE NOT NULL, " +
                "\"optionalDate\" DATE NULL, " +
                "\"optionalLocalDate\" DATE NULL, " +
                "\"optionalLocalDateList\" ARRAY(DATE NULL) NULL, " +
                "\"localDateIso8601\" DATE NULL, " +
                "\"localDateIso8601List\" ARRAY(DATE NULL) NULL, " +
                "\"dateAsIso8601List\" ARRAY(DATE NULL) NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(DATE NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(DATE NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(DATE NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(DATE NOT NULL) NULL, " +
                "\"dateAsString\" DATE NULL" +
                ")";
    }
    
    /**
     * Publishes DateTestRecord messages to Kafka using JSON Schema serialization.
     * Converts LocalDate objects to integers (days since epoch) for Kafka Connect Date logical type.
     */
    private void publishMessages(List<DateTestRecord> records) throws Exception {
        for (DateTestRecord record : records) {
            String key = "date-test-key-" + record.getRecordId();
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

    private String toIsoDate(java.util.Date date) {
        if (date == null) {
            return null;
        }
        if (date instanceof Date) {
            LocalDate ld = ((Date) date).toLocalDate();
            return ISO_DATE_FORMATTER.format(ld);
        }
        return ISO_DATE_FORMATTER.format(date.toInstant().atZone(ZoneId.systemDefault()));
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
            "SELECT \"recordId\", \"requiredDate\", \"optionalDate\", \"optionalLocalDate\", \"optionalLocalDateList\", \"localDateIso8601\", \"localDateIso8601List\", \"dateAsString\", \"dateAsIso8601List\", " +
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
                Date actualRequiredDate = rs.getDate("requiredDate");
                Date actualOptionalDate = rs.getDate("optionalDate");
                Date actualOptionalLocalDate = rs.getDate("optionalLocalDate");
                Array actualOptionalLocalDateListArray = rs.getArray("optionalLocalDateList");
                Date actualLocalDateIso8601 = rs.getDate("localDateIso8601");
                Array actualLocalDateIso8601ListArray = rs.getArray("localDateIso8601List");
                Date actualDateAsString = rs.getDate("dateAsString");
                Array actualDateAsIso8601ListArray = rs.getArray("dateAsIso8601List");
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                String expectedRequiredDateIso = toIsoDate(expected.getRequiredDate());
                String actualRequiredDateIso = toIsoDate(actualRequiredDate);
                assertEquals(expectedRequiredDateIso, actualRequiredDateIso, 
                    "RequiredDate mismatch at index " + recordIndex);
                
                // Null handling verification for optional date
                if (expected.getOptionalDate() == null) {
                    assertNull(actualOptionalDate, 
                        "OptionalDate should be null at index " + recordIndex);
                } else {
                    String expectedOptionalDateIso = toIsoDate(expected.getOptionalDate());
                    String actualOptionalDateIso = toIsoDate(actualOptionalDate);
                    assertEquals(expectedOptionalDateIso, actualOptionalDateIso,
                        "OptionalDate mismatch at index " + recordIndex);
                }

                // localDateIso8601 verification
                if (expected.getLocalDateIso8601() == null) {
                    assertNull(actualLocalDateIso8601, 
                        "localDateIso8601 should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getLocalDateIso8601().toString(), actualLocalDateIso8601.toLocalDate().toString(),
                        "localDateIso8601 mismatch at index " + recordIndex);
                }

                // localDateIso8601List verification
                if (expected.getLocalDateIso8601List() == null) {
                    assertNull(actualLocalDateIso8601ListArray, 
                        "localDateIso8601List should be null at index " + recordIndex);
                } else {
                    assertNotNull(actualLocalDateIso8601ListArray, 
                        "localDateIso8601List should not be null at index " + recordIndex);
                    int baseType3 = actualLocalDateIso8601ListArray.getBaseType();
                    assertEquals(Types.DATE, baseType3,
                        "localDateIso8601List should have base type DATE (91) at index " + recordIndex);

                    Date[] arrayElements3 = (Date[]) actualLocalDateIso8601ListArray.getArray();
                    List<String> actualIsoList2 = new ArrayList<>();
                    for (Date d : arrayElements3) {
                        actualIsoList2.add(d == null ? null : d.toLocalDate().toString());
                    }
                    List<String> expectedIsoList2 = new ArrayList<>();
                    for (LocalDate d : expected.getLocalDateIso8601List()) {
                        expectedIsoList2.add(d == null ? null : d.toString());
                    }
                    assertEquals(expectedIsoList2, actualIsoList2,
                        "localDateIso8601List mismatch at index " + recordIndex);
                }

                // dateAsString verification
                if (expected.getDateAsString() == null) {
                    assertNull(actualDateAsString, 
                        "dateAsString should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getDateAsString(), actualDateAsString.toLocalDate().toString(),
                        "dateAsString mismatch at index " + recordIndex);
                }

                // dateAsIso8601List verification
                if (expected.getDateAsIso8601List() == null) {
                    assertNull(actualDateAsIso8601ListArray,
                        "dateAsIso8601List should be null at index " + recordIndex);
                } else {
                    assertNotNull(actualDateAsIso8601ListArray,
                        "dateAsIso8601List should not be null at index " + recordIndex);
                    int baseType4 = actualDateAsIso8601ListArray.getBaseType();
                    assertEquals(Types.DATE, baseType4,
                        "dateAsIso8601List should have base type DATE (91) at index " + recordIndex);

                    Date[] arrayElements4 = (Date[]) actualDateAsIso8601ListArray.getArray();
                    List<String> actualIsoList3 = new ArrayList<>();
                    for (Date d : arrayElements4) {
                        actualIsoList3.add(d == null ? null : d.toLocalDate().toString());
                    }
                    List<String> expectedIsoList3 = expected.getDateAsIso8601List();
                    assertEquals(expectedIsoList3, actualIsoList3,
                        "dateAsIso8601List mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyDateArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex);
                    
                verifyDateArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex);
                
                // Optional list verification
                verifyDateArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex);
                
                // Optional list with non-null elements verification
                verifyDateArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex);

                // optionalLocalDate verification
                if (expected.getOptionalLocalDate() == null) {
                    assertNull(actualOptionalLocalDate, 
                        "OptionalLocalDate should be null at index " + recordIndex);
                } else {
                    String expectedOptionalLocalDateIso = expected.getOptionalLocalDate().toString();
                    String actualOptionalLocalDateIso = actualOptionalLocalDate.toLocalDate().toString();
                    assertEquals(expectedOptionalLocalDateIso, actualOptionalLocalDateIso,
                        "OptionalLocalDate mismatch at index " + recordIndex);
                }

                // optionalLocalDateList verification
                if (expected.getOptionalLocalDateList() == null) {
                    assertNull(actualOptionalLocalDateListArray, 
                        "optionalLocalDateList should be null at index " + recordIndex);
                } else {
                    assertNotNull(actualOptionalLocalDateListArray, 
                        "optionalLocalDateList should not be null at index " + recordIndex);
                    int baseType2 = actualOptionalLocalDateListArray.getBaseType();
                    assertEquals(Types.DATE, baseType2,
                        "optionalLocalDateList should have base type DATE (91) at index " + recordIndex);

                    Date[] arrayElements2 = (Date[]) actualOptionalLocalDateListArray.getArray();
                    List<String> actualIsoList = new ArrayList<>();
                    for (Date d : arrayElements2) {
                        if (d == null) {
                            actualIsoList.add(null);
                        } else {
                            actualIsoList.add(d.toLocalDate().toString());
                        }
                    }

                    List<String> expectedIsoList = new ArrayList<>();
                    for (LocalDate d : expected.getOptionalLocalDateList()) {
                        expectedIsoList.add(d == null ? null : d.toString());
                    }

                    assertEquals(expectedIsoList, actualIsoList,
                        "optionalLocalDateList mismatch at index " + recordIndex);
                }
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
    
    /**
     * Verifies a date array field using Array object instead of string parsing.
     */
    private void verifyDateArray(String fieldName, List<java.util.Date> expected, Array actualArray, 
                               int recordIndex) throws SQLException {
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
        List<java.util.Date> actualList = new ArrayList<>();
        
        for (Date date : arrayElements) {
            if (date != null) {
                actualList.add(date);
            } else {
                actualList.add(null);
            }
        }

        assertEquals(expected, actualList, fieldName + " mismatch at index " + recordIndex);
    }

}
