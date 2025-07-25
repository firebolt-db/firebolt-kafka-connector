package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.TimestampTestRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
public class TimestampSerializerTest extends BaseIntegrationTest {

    private static final String TOPIC_NAME = "timestamp-test-topic";
    private static final String TABLE_NAME = "timestamp_test_table";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, Object> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("timestamp-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         timestampTableSchema(), jsonTimestampSchema());
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
    void testTimestampSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);
        
        List<TimestampTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        // For sub-millisecond precision tests (records 13-14), we need to use truncated expected values
        // since Kafka Connect's Timestamp logical type only supports millisecond precision
        List<TimestampTestRecord> expectedRecords = createExpectedRecordsWithTruncatedNanoseconds(testRecords);

        verifyTimestampRecordsInFirebolt(expectedRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<TimestampTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with recent timestamps
            aValidTestRecord(2)
                .requiredTimestamp(LocalDateTime.of(2024, 12, 31, 23, 59, 59))
                .optionalTimestamp(LocalDateTime.of(2025, 1, 1, 0, 0, 0))
                .build(),

            // Record with historical timestamps
            aValidTestRecord(3)
                .requiredTimestamp(LocalDateTime.of(1970, 1, 1, 0, 0, 0))  // Unix epoch
                .optionalTimestamp(LocalDateTime.of(2000, 1, 1, 0, 0, 0))  // Y2K
                .build(),

            // Record with null optional timestamp
            aValidTestRecord(4)
                .optionalTimestamp(null)
                .build(),

            // Record with empty lists
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .requiredListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with nullable elements in list
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDateTime.of(2024, 1, 1, 12, 0, 0), null, LocalDateTime.of(2024, 12, 31, 23, 59, 59)))
                .build(),

            // Record with various timestamp ranges
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(
                    null, LocalDateTime.of(1970, 1, 1, 0, 0, 0), LocalDateTime.of(2024, 6, 15, 14, 30, 45)))
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2023, 1, 1, 9, 15, 30), LocalDateTime.of(2024, 6, 15, 18, 45, 15), LocalDateTime.of(2025, 12, 31, 23, 59, 59)))
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
                .optionalList(Arrays.asList(LocalDateTime.of(2024, 3, 15, 10, 30, 0), null, LocalDateTime.of(2024, 9, 30, 16, 45, 30)))
                .optionalListWithNonNullElements(Arrays.asList(LocalDateTime.of(2024, 4, 1, 8, 0, 0), LocalDateTime.of(2024, 8, 31, 17, 30, 45)))
                .build(),

            // Record with leap year timestamps (February 29th)
            aValidTestRecord(11)
                .requiredTimestamp(LocalDateTime.of(2024, 2, 29, 12, 0, 0))  // Leap year timestamp
                .optionalTimestamp(LocalDateTime.of(2020, 2, 29, 23, 59, 59))  // Another leap year timestamp
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDateTime.of(2024, 2, 29, 6, 30, 15), null, LocalDateTime.of(2020, 2, 29, 18, 45, 30), null, LocalDateTime.of(2000, 2, 29, 12, 0, 0)))
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 2, 29, 9, 15, 45), LocalDateTime.of(2020, 2, 29, 15, 30, 0), LocalDateTime.of(2016, 2, 29, 21, 45, 15)))
                .optionalList(Arrays.asList(
                    null, LocalDateTime.of(2024, 2, 29, 3, 15, 30), null, LocalDateTime.of(2012, 2, 29, 14, 30, 45), null))
                .optionalListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2008, 2, 29, 11, 0, 0), LocalDateTime.of(2004, 2, 29, 20, 30, 15)))
                .build(),

            // Record with large lists (100 elements each for performance testing)
            aValidTestRecord(12)
                .requiredTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 0))
                .optionalTimestamp(LocalDateTime.of(2024, 12, 31, 23, 59, 59))
                .requiredListWithNullableElements(createLargeTimestampListWithNulls(100))
                .requiredListWithNonNullElements(createLargeTimestampListWithoutNulls(100))
                .optionalList(createOptionalLargeTimestampListWithNulls(100))  // Use version with nulls
                .optionalListWithNonNullElements(createOptionalLargeTimestampList(80))  // Use version without nulls
                .build(),

            // Record with microsecond precision (will be truncated to milliseconds by Kafka Connect)
            aValidTestRecord(13)
                .requiredTimestamp(LocalDateTime.of(2024, 1, 15, 14, 30, 45, 123456000))  // 123.456 ms -> 123 ms (truncated)
                .optionalTimestamp(LocalDateTime.of(2024, 6, 30, 9, 15, 30, 987654000))   // 987.654 ms -> 987 ms (truncated)
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDateTime.of(2024, 3, 1, 10, 0, 0, 500000000),     // 500.000 ms -> 500 ms (truncated)
                    null,
                    LocalDateTime.of(2024, 8, 15, 16, 45, 12, 123456000)))  // 123.456 ms -> 123 ms (truncated)
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 5, 20, 8, 30, 45, 750000000),   // 750.000 ms -> 750 ms (truncated)
                    LocalDateTime.of(2024, 9, 10, 20, 15, 30, 999999000))) // 999.999 ms -> 999 ms (truncated)
                .optionalList(Arrays.asList(
                    LocalDateTime.of(2024, 2, 14, 12, 0, 0, 111111000),    // 111.111 ms -> 111 ms (truncated)
                    null,
                    LocalDateTime.of(2024, 7, 4, 18, 30, 45, 666666000)))  // 666.666 ms -> 666 ms (truncated)
                .optionalListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 4, 10, 6, 45, 15, 333333000),   // 333.333 ms -> 333 ms (truncated)
                    LocalDateTime.of(2024, 10, 25, 22, 0, 0, 888888000)))  // 888.888 ms -> 888 ms (truncated)
                // Test microsecond precision fields - these should preserve full precision
                .microsecondTimestamp(1705334445123456L) // 2024-01-15T14:30:45.123456 in microseconds since epoch
                .timestampStringArray(Arrays.asList(
                    "2024-03-01T10:00:00.500123",   // 500.123 milliseconds precision
                    "2024-08-15T16:45:12.123456",   // 123.456 milliseconds precision  
                    "2024-05-20T08:30:45.750789"))  // 750.789 milliseconds precision
                .build(),

            // Record with nanosecond precision (should be truncated to milliseconds)
            // NOTE: Kafka Connect's Timestamp logical type only supports millisecond precision,
            // so any sub-millisecond data (microseconds/nanoseconds) will be truncated to milliseconds
            aValidTestRecord(14)
                .requiredTimestamp(LocalDateTime.of(2024, 1, 15, 14, 30, 45, 123456789))  // Should become 123000000 (123 milliseconds)
                .optionalTimestamp(LocalDateTime.of(2024, 6, 30, 9, 15, 30, 987654321))   // Should become 987000000 (987 milliseconds)
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDateTime.of(2024, 3, 1, 10, 0, 0, 500000123),     // Should become 500000000 (500 milliseconds)
                    null,
                    LocalDateTime.of(2024, 8, 15, 16, 45, 12, 999999999))) // Should become 999000000 (999 milliseconds)
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 5, 20, 8, 30, 45, 750000456),   // Should become 750000000 (750 milliseconds)
                    LocalDateTime.of(2024, 9, 10, 20, 15, 30, 111111111))) // Should become 111000000 (111 milliseconds)
                .optionalList(Arrays.asList(
                    LocalDateTime.of(2024, 2, 14, 12, 0, 0, 222222222),    // Should become 222000000 (222 milliseconds)
                    null,
                    LocalDateTime.of(2024, 7, 4, 18, 30, 45, 888888888)))  // Should become 888000000 (888 milliseconds)
                .optionalListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 4, 10, 6, 45, 15, 444444444),   // Should become 444000000 (444 milliseconds)
                    LocalDateTime.of(2024, 10, 25, 22, 0, 0, 777777777)))  // Should become 777000000 (777 milliseconds)
                // Test nanosecond precision fields - microsecond precision should be preserved
                .microsecondTimestamp(1719485730987654L) // 2024-06-30T09:15:30.987654 in microseconds since epoch
                .timestampStringArray(Arrays.asList(
                    "2024-03-01T10:00:00.500999",   // Maximum sub-millisecond precision
                    "2024-08-15T16:45:12.999999",   // Maximum microsecond precision
                    "2024-02-14T12:00:00.222333",   // Mixed precision values
                    "2024-07-04T18:30:45.888777"))  // Different microsecond values
                .build()
        );
    }
    
    /**
     * Helper method to create a large list with nullable timestamp elements.
     */
    private List<LocalDateTime> createLargeTimestampListWithNulls(int size) {
        List<LocalDateTime> result = new ArrayList<>();
        LocalDateTime baseTimestamp = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : baseTimestamp.plusHours(i));  // Every 5th element is null
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null timestamp elements.
     */
    private List<LocalDateTime> createLargeTimestampListWithoutNulls(int size) {
        List<LocalDateTime> result = new ArrayList<>();
        LocalDateTime baseTimestamp = LocalDateTime.of(2023, 1, 1, 0, 0, 0);
        for (int i = 0; i < size; i++) {
            result.add(baseTimestamp.plusMinutes(i * 30));  // Every 30 minutes
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with different timestamp range.
     * Used for both optionalList and optionalListWithNonNullElements, so no nulls.
     */
    private List<LocalDateTime> createOptionalLargeTimestampList(int size) {
        List<LocalDateTime> result = new ArrayList<>();
        LocalDateTime baseTimestamp = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        for (int i = 0; i < size; i++) {
            result.add(baseTimestamp.plusHours(i * 2));  // Every 2 hours
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list WITH null values for testing nullable lists.
     */
    private List<LocalDateTime> createOptionalLargeTimestampListWithNulls(int size) {
        List<LocalDateTime> result = new ArrayList<>();
        LocalDateTime baseTimestamp = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        for (int i = 0; i < size; i++) {
            // Every 7th element is null to test null handling in optional lists
            if (i % 7 == 0) {
                result.add(null);
            } else {
                result.add(baseTimestamp.plusHours(i * 2));  // Every 2 hours
            }
        }
        return result;
    }

    private TimestampTestRecord.TimestampTestRecordBuilder aValidTestRecord(int recordId) {
        return TimestampTestRecord.builder()
                .recordId(recordId)
                .requiredTimestamp(LocalDateTime.of(2024, 1, 15, 14, 30, 45))
                .optionalTimestamp(LocalDateTime.of(2024, 2, 28, 16, 45, 30))
                .requiredListWithNullableElements(Arrays.asList(
                    LocalDateTime.of(2024, 3, 1, 9, 0, 0), null, LocalDateTime.of(2024, 3, 31, 17, 30, 15), null, LocalDateTime.of(2024, 4, 15, 12, 15, 45)))
                .requiredListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 5, 1, 8, 30, 0), LocalDateTime.of(2024, 6, 15, 13, 45, 30), LocalDateTime.of(2024, 7, 31, 19, 15, 0)))
                .optionalList(Arrays.asList(
                    LocalDateTime.of(2024, 8, 1, 7, 0, 0), LocalDateTime.of(2024, 9, 15, 14, 30, 45), LocalDateTime.of(2024, 10, 31, 20, 45, 15)))
                .optionalListWithNonNullElements(Arrays.asList(
                    LocalDateTime.of(2024, 11, 1, 6, 15, 30), LocalDateTime.of(2024, 11, 15, 15, 0, 0), LocalDateTime.of(2024, 12, 1, 21, 30, 45)))
                // Default values for new microsecond precision fields
                .microsecondTimestamp(1705334445123456L) // 2024-01-15T14:30:45.123456 in microseconds since epoch
                .timestampStringArray(Arrays.asList(
                    "2024-01-15T14:30:45.123456", 
                    "2024-02-28T16:45:30.987654", 
                    "2024-03-15T12:00:00.500000"));
    }
    
    /**
     * Creates the Firebolt table with TIMESTAMP columns.
     */
    private Supplier<String> timestampTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredTimestamp\" TIMESTAMP NOT NULL, " +
                "\"optionalTimestamp\" TIMESTAMP NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TIMESTAMP NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TIMESTAMP NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TIMESTAMP NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TIMESTAMP NOT NULL) NULL, " +
                "\"microsecondTimestamp\" TIMESTAMP NOT NULL, " +
                "\"timestampStringArray\" ARRAY(TIMESTAMP NOT NULL) NOT NULL" +
                ")";
    }
    
    private Supplier<String> jsonTimestampSchema() {
        return () -> "{" +
                "\"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
                "\"title\": \"Timestamp Test Record\"," +
                "\"type\": \"object\"," +
                "\"additionalProperties\": false," +
                "\"properties\": {" +
                    "\"recordId\": {" +
                        "\"type\": \"integer\"," +
                        "\"description\": \"Record identification number\"" +
                    "}," +
                    "\"requiredTimestamp\": {" +
                        "\"type\": \"integer\"," +
                        "\"connect.type\": \"int64\"," +
                        "\"connect.version\": 1," +
                        "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"," +
                        "\"description\": \"Required timestamp field\"" +
                    "}," +
                    "\"optionalTimestamp\": {" +
                        "\"oneOf\": [" +
                            "{\"type\": \"null\"}," +
                            "{" +
                                "\"type\": \"integer\"," +
                                "\"connect.type\": \"int64\"," +
                                "\"connect.version\": 1," +
                                "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional timestamp field\"" +
                    "}," +
                    "\"requiredListWithNullableElements\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"oneOf\": [" +
                                "{\"type\": \"null\"}," +
                                "{" +
                                    "\"type\": \"integer\"," +
                                    "\"connect.type\": \"int64\"," +
                                    "\"connect.version\": 1," +
                                    "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"" +
                                "}" +
                            "]" +
                        "}," +
                        "\"description\": \"Required list with nullable elements\"" +
                    "}," +
                    "\"requiredListWithNonNullElements\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"type\": \"integer\"," +
                            "\"connect.type\": \"int64\"," +
                            "\"connect.version\": 1," +
                            "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"" +
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
                                            "\"connect.type\": \"int64\"," +
                                            "\"connect.version\": 1," +
                                            "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"" +
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
                                    "\"connect.type\": \"int64\"," +
                                    "\"connect.version\": 1," +
                                    "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"" +
                                "}" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional list with non-null elements\"" +
                    "}" +
                "}," +
                "\"required\": [\"recordId\", \"requiredTimestamp\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]" +
                "}";
    }
    
    /**
     * Publishes TimestampTestRecord messages to Kafka using JSON Schema serialization.
     * Converts LocalDateTime objects to longs (milliseconds since epoch) for Kafka Connect Timestamp logical type.
     */
    private void publishMessages(List<TimestampTestRecord> records) throws Exception {
        for (TimestampTestRecord record : records) {
            String key = "timestamp-test-key-" + record.getRecordId();
            
            // Convert LocalDateTime objects to longs (milliseconds since epoch) for Kafka Connect Timestamp logical type
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("recordId", record.getRecordId());
            
            recordMap.put("requiredTimestamp", localDateTimeToEpochMillis(record.getRequiredTimestamp()));
            recordMap.put("optionalTimestamp", record.getOptionalTimestamp() != null ? localDateTimeToEpochMillis(record.getOptionalTimestamp()) : null);
                
            // Convert timestamp arrays
            recordMap.put("requiredListWithNullableElements", convertTimestampListToLongList(record.getRequiredListWithNullableElements()));
            recordMap.put("requiredListWithNonNullElements", convertTimestampListToLongList(record.getRequiredListWithNonNullElements()));
            recordMap.put("optionalList", record.getOptionalList() != null ? convertTimestampListToLongList(record.getOptionalList()) : null);
            recordMap.put("optionalListWithNonNullElements", record.getOptionalListWithNonNullElements() != null ? convertTimestampListToLongList(record.getOptionalListWithNonNullElements()) : null);
            
            // Convert new microsecond precision fields
            Long microsecondValue = record.getMicrosecondTimestamp();
            List<String> stringArrayValue = record.getTimestampStringArray();

            recordMap.put("microsecondTimestamp", microsecondValue); // Already in microseconds, send as-is
            recordMap.put("timestampStringArray", stringArrayValue); // Already formatted strings, send as-is
            
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
     * Converts LocalDateTime to milliseconds since Unix epoch (1970-01-01T00:00:00Z).
     */
    private long localDateTimeToEpochMillis(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
    
    /**
     * Creates expected test records with sub-millisecond precision truncated to milliseconds
     * (as Kafka Connect's Timestamp logical type only supports millisecond precision).
     */
    private List<TimestampTestRecord> createExpectedRecordsWithTruncatedNanoseconds(List<TimestampTestRecord> originalRecords) {
        List<TimestampTestRecord> expectedRecords = new ArrayList<>();
        
        for (TimestampTestRecord record : originalRecords) {
            TimestampTestRecord.TimestampTestRecordBuilder builder = TimestampTestRecord.builder()
                .recordId(record.getRecordId())
                .requiredTimestamp(truncateToMicroseconds(record.getRequiredTimestamp()))
                .optionalTimestamp(truncateToMicroseconds(record.getOptionalTimestamp()));
            
            // Handle arrays with potential null elements
            if (record.getRequiredListWithNullableElements() != null) {
                builder.requiredListWithNullableElements(
                    record.getRequiredListWithNullableElements().stream()
                        .map(this::truncateToMicroseconds)
                        .collect(Collectors.toList()));
            }
            
            if (record.getRequiredListWithNonNullElements() != null) {
                builder.requiredListWithNonNullElements(
                    record.getRequiredListWithNonNullElements().stream()
                        .map(this::truncateToMicroseconds)
                        .collect(Collectors.toList()));
            }
            
            if (record.getOptionalList() != null) {
                builder.optionalList(
                    record.getOptionalList().stream()
                        .map(this::truncateToMicroseconds)
                        .collect(Collectors.toList()));
            }
            
            if (record.getOptionalListWithNonNullElements() != null) {
                builder.optionalListWithNonNullElements(
                    record.getOptionalListWithNonNullElements().stream()
                        .map(this::truncateToMicroseconds)
                        .collect(Collectors.toList()));
            }
            
            // Add the new microsecond precision fields (these should be preserved as-is)
            builder.microsecondTimestamp(record.getMicrosecondTimestamp());
            builder.timestampStringArray(record.getTimestampStringArray());
            
            expectedRecords.add(builder.build());
        }
        
        return expectedRecords;
    }
    
    /**
     * Truncates LocalDateTime nanoseconds to millisecond precision.
     * Kafka Connect's Timestamp logical type only supports millisecond granularity.
     */
    private LocalDateTime truncateToMicroseconds(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        // Truncate nanoseconds to milliseconds (keep only the first 3 digits, set last 6 to 0)
        int truncatedNanos = (dateTime.getNano() / 1_000_000) * 1_000_000;
        return dateTime.withNano(truncatedNanos);
    }
    
    /**
     * Converts a list of LocalDateTime objects to a list of longs (milliseconds since epoch).
     */
    private List<Long> convertTimestampListToLongList(List<LocalDateTime> timestampList) {
        if (timestampList == null) {
            return null;
        }
        return timestampList.stream()
            .map(timestamp -> timestamp != null ? localDateTimeToEpochMillis(timestamp) : null)
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Verifies that the published timestamp records exist in the Firebolt table with correct null handling.
     */
    private void verifyTimestampRecordsInFirebolt(List<TimestampTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredTimestamp\", \"optionalTimestamp\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\", \"microsecondTimestamp\", \"timestampStringArray\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                TimestampTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                
                // Debug logging for precision verification
                if (recordIndex == 12 || recordIndex == 13) { // Records with microsecond/nanosecond precision
                    java.sql.Timestamp sqlTimestamp = rs.getTimestamp("requiredTimestamp");
                    log.info("DEBUG precision test - Record {}: SQL Timestamp = {}, Nanos = {}", 
                        recordIndex, sqlTimestamp, sqlTimestamp != null ? sqlTimestamp.getNanos() : "null");
                }
                
                LocalDateTime actualRequiredTimestamp = rs.getTimestamp("requiredTimestamp") != null ? 
                    rs.getTimestamp("requiredTimestamp").toLocalDateTime() : null;
                LocalDateTime actualOptionalTimestamp = rs.getTimestamp("optionalTimestamp") != null ? 
                    rs.getTimestamp("optionalTimestamp").toLocalDateTime() : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Retrieve new microsecond precision fields
                java.sql.Timestamp actualMicrosecondTimestamp = rs.getTimestamp("microsecondTimestamp");
                Array actualTimestampStringArray = rs.getArray("timestampStringArray");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredTimestamp(), actualRequiredTimestamp, 
                    "RequiredTimestamp mismatch at index " + recordIndex);
                
                // Null handling verification for optional timestamp
                if (expected.getOptionalTimestamp() == null) {
                    assertNull(actualOptionalTimestamp, 
                        "OptionalTimestamp should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalTimestamp(), actualOptionalTimestamp, 
                        "OptionalTimestamp mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyTimestampArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyTimestampArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray, 
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyTimestampArray("optionalList", 
                        expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                }
                
                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray, 
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyTimestampArray("optionalListWithNonNullElements", 
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                }
                
                // Verify microsecond precision fields (all records should have these fields)
                verifyMicrosecondTimestamp(expected.getMicrosecondTimestamp(), actualMicrosecondTimestamp, recordIndex);
                verifyTimestampStringArray(expected.getTimestampStringArray(), actualTimestampStringArray.toString(), recordIndex);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
    }
    
    /**
     * Verifies a timestamp array field using Array object instead of string parsing.
     */
    private void verifyTimestampArray(String fieldName, List<LocalDateTime> expected, Array actualArray, 
                                    int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is TIMESTAMP (Types.TIMESTAMP = 93)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.TIMESTAMP, baseType,
            fieldName + " should have base type TIMESTAMP (93) at index " + recordIndex);

        // Get the array as Timestamp array and convert to List<LocalDateTime>
        java.sql.Timestamp[] arrayElements = (java.sql.Timestamp[]) actualArray.getArray();
        List<LocalDateTime> actualList = Arrays.stream(arrayElements)
            .map(timestamp -> timestamp != null ? timestamp.toLocalDateTime() : null)
            .collect(Collectors.toList());

        // Direct list comparison
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            LocalDateTime expectedElement = expected.get(i);
            LocalDateTime actualElement = actualList.get(i);
            
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
     * Verifies microsecond precision timestamp field by comparing the expected Long (microseconds since epoch)
     * with the actual Timestamp retrieved from Firebolt.
     */
    private void verifyMicrosecondTimestamp(Long expectedMicroseconds, java.sql.Timestamp actualTimestamp, int recordIndex) {
        assertNotNull(expectedMicroseconds, "Expected microsecondTimestamp should not be null at index " + recordIndex);
        assertNotNull(actualTimestamp, "Actual microsecondTimestamp should not be null at index " + recordIndex);
        
        // Convert expected microseconds to expected timestamp
        long expectedMillis = expectedMicroseconds / 1000; // Convert microseconds to milliseconds
        int expectedMicros = (int) (expectedMicroseconds % 1000000); // Remaining microseconds
        
        // Firebolt should preserve microsecond precision
        long actualMillis = actualTimestamp.getTime();
        int actualNanos = actualTimestamp.getNanos();
        int actualMicros = actualNanos / 1000; // Convert nanoseconds to microseconds
        
        // Account for timezone offset (Firebolt may apply different offsets: +1, +2, or +3 hours)
        long timezoneOffset1Hr = 1 * 60 * 60 * 1000; // 1 hour in milliseconds
        long timezoneOffset2Hr = 2 * 60 * 60 * 1000; // 2 hours in milliseconds
        long timezoneOffset3Hr = 3 * 60 * 60 * 1000; // 3 hours in milliseconds
        
        long adjustedExpected1Hr = expectedMillis - timezoneOffset1Hr;
        long adjustedExpected2Hr = expectedMillis - timezoneOffset2Hr;
        long adjustedExpected3Hr = expectedMillis - timezoneOffset3Hr;
        
        // Check if the actual value matches any of the expected timezone offsets
        boolean matchesTimezone = (actualMillis == adjustedExpected1Hr) || 
                                 (actualMillis == adjustedExpected2Hr) || 
                                 (actualMillis == adjustedExpected3Hr);
        
        if (!matchesTimezone) {
            // If no offset works, show detailed error
            long actualOffset = (expectedMillis - actualMillis) / (60 * 60 * 1000);
            throw new AssertionError(String.format(
                "Microsecond timestamp offset unexpected at index %d. Expected: %d, Actual: %d, " +
                "Actual offset: %d hours. Expected 1, 2, or 3 hour offset.",
                recordIndex, expectedMillis, actualMillis, actualOffset));
        }
        
        assertEquals(expectedMicros, actualMicros, 
            "Microsecond timestamp microseconds precision mismatch at index " + recordIndex);
    }
    
    /**
     * Verifies timestamp string array by parsing both expected and actual string arrays
     * and comparing their timestamp values.
     */
    private void verifyTimestampStringArray(List<String> expectedStrings, String actualArrayString, int recordIndex) {
        assertNotNull(expectedStrings, "Expected timestampStringArray should not be null at index " + recordIndex);
        assertNotNull(actualArrayString, "Actual timestampStringArray should not be null at index " + recordIndex);
        
        // Parse Firebolt array string (e.g., "[2024-01-15 14:30:45.123456,2024-02-28 16:45:30.987654]")
        List<String> actualParsedStrings = parseFireboltTimestampStringArray(actualArrayString);
        
        assertEquals(expectedStrings.size(), actualParsedStrings.size(),
            "TimestampStringArray size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expectedStrings.size(); i++) {
            String expectedStr = expectedStrings.get(i);
            String actualStr = actualParsedStrings.get(i);
            
            // Convert both to timestamps for comparison (accounting for format differences)
            String normalizedExpected = expectedStr.replace("T", " "); // Convert ISO format to Firebolt format
            String normalizedActual = actualStr;
            
            // Normalize fractional seconds (Firebolt trims trailing zeros)
            normalizedExpected = normalizeFractionalSeconds(normalizedExpected);
            normalizedActual = normalizeFractionalSeconds(normalizedActual);
            
            assertEquals(normalizedExpected, normalizedActual,
                "TimestampStringArray element " + i + " mismatch at index " + recordIndex);
        }
        
    }
    
    /**
     * Parses Firebolt timestamp string array format into individual timestamp strings.
     * Example: "[2024-01-15 14:30:45.123456,2024-02-28 16:45:30.987654]"
     */
    private List<String> parseFireboltTimestampStringArray(String arrayString) {
        List<String> result = new ArrayList<>();
        
        if (arrayString == null || arrayString.trim().isEmpty()) {
            return result;
        }
        
        // Remove brackets (both [] and {} formats) and split by comma
        String cleaned = arrayString.trim().replaceAll("^[\\[{]|[\\]}]$", "");
        if (cleaned.isEmpty()) {
            return result;
        }
        
        String[] elements = cleaned.split(",");
        for (String element : elements) {
            String trimmed = element.trim();
            // Remove quotes if present
            if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                trimmed = trimmed.substring(1, trimmed.length() - 1);
            }
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        
        return result;
    }
    
    /**
     * Normalizes fractional seconds in timestamp strings for consistent comparison.
     */
    private String normalizeFractionalSeconds(String timestamp) {
        if (timestamp == null) {
            return null;
        }
        
        // If the timestamp has fractional seconds, normalize to 6 digits (microseconds)
        if (timestamp.contains(".")) {
            String[] parts = timestamp.split("\\.");
            if (parts.length == 2) {
                String baseTime = parts[0];
                String fractional = parts[1];
                
                // Pad or truncate fractional part to 6 digits
                if (fractional.length() < 6) {
                    fractional = fractional + "0".repeat(6 - fractional.length());
                } else if (fractional.length() > 6) {
                    fractional = fractional.substring(0, 6);
                }
                
                return baseTime + "." + fractional;
            }
        }
        
        return timestamp;
    }
} 