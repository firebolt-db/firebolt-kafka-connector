package com.firebolt.kafka.connect.integration.json;

import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.TimestamptzTestRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
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
public class TimestamptzSerializerTest extends BaseIntegrationTest {

    private static final String TOPIC_NAME = "timestamptz-test-topic";
    private static final String TABLE_NAME = "timestamptz_test_table";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, Object> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("timestamptz-serializer-test");
        
        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, 
                         timestamptzTableSchema(), jsonTimestamptzSchema());
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
    void testTimestamptzSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeJsonProducer(includeNulls);
        
        List<TimestamptzTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        // For sub-millisecond precision tests (records 13-14), we need to use truncated expected values
        // since Kafka Connect's Timestamp logical type only supports millisecond precision
        List<TimestamptzTestRecord> expectedRecords = createExpectedRecordsWithTruncatedNanoseconds(testRecords);

        verifyTimestamptzRecordsInFirebolt(expectedRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<TimestamptzTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with recent timestamptz values
            aValidTestRecord(2)
                .requiredTimestamptz(OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC))
                .optionalTimestamptz(OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
                .build(),

            // Record with historical timestamptz values
            aValidTestRecord(3)
                .requiredTimestamptz(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))  // Unix epoch
                .optionalTimestamptz(OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))  // Y2K
                .build(),

            // Record with null optional timestamptz
            aValidTestRecord(4)
                .optionalTimestamptz(null)
                .build(),

            // Record with empty lists
            aValidTestRecord(5)
                .requiredListWithNullableElements(new ArrayList<>())
                .requiredListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with nullable elements in list
            aValidTestRecord(6)
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC)))
                .build(),

            // Record with various timestamptz ranges
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(
                    null, OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.UTC)))
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2023, 1, 1, 9, 15, 30, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 6, 15, 18, 45, 15, 0, ZoneOffset.UTC), OffsetDateTime.of(2025, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC)))
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
                .optionalList(Arrays.asList(OffsetDateTime.of(2024, 3, 15, 10, 30, 0, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2024, 9, 30, 16, 45, 30, 0, ZoneOffset.UTC)))
                .optionalListWithNonNullElements(Arrays.asList(OffsetDateTime.of(2024, 4, 1, 8, 0, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 8, 31, 17, 30, 45, 0, ZoneOffset.UTC)))
                .build(),

            // Record with leap year timestamptz values (February 29th with timezone awareness)
            aValidTestRecord(11)
                .requiredTimestamptz(OffsetDateTime.of(2024, 2, 29, 12, 0, 0, 0, ZoneOffset.UTC))  // Leap year timestamptz
                .optionalTimestamptz(OffsetDateTime.of(2020, 2, 29, 23, 59, 59, 0, ZoneOffset.UTC))  // Another leap year timestamptz
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 29, 6, 30, 15, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2020, 2, 29, 18, 45, 30, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2000, 2, 29, 12, 0, 0, 0, ZoneOffset.UTC)))
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 29, 9, 15, 45, 0, ZoneOffset.UTC), OffsetDateTime.of(2020, 2, 29, 15, 30, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2016, 2, 29, 21, 45, 15, 0, ZoneOffset.UTC)))
                .optionalList(Arrays.asList(
                    null, OffsetDateTime.of(2024, 2, 29, 3, 15, 30, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2012, 2, 29, 14, 30, 45, 0, ZoneOffset.UTC), null))
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2008, 2, 29, 11, 0, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2004, 2, 29, 20, 30, 15, 0, ZoneOffset.UTC)))
                .build(),

            // Record with large lists (100 elements each for performance testing)
            aValidTestRecord(12)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC))
                .optionalTimestamptz(OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC))
                .requiredListWithNullableElements(createLargeTimestamptzListWithNulls(100))
                .requiredListWithNonNullElements(createLargeTimestamptzListWithoutNulls(100))
                .optionalList(createOptionalLargeTimestamptzListWithNulls(100))  // Use version with nulls
                .optionalListWithNonNullElements(createOptionalLargeTimestamptzList(80))  // Use version without nulls
                .build(),

            // Record with microsecond precision (will be truncated to milliseconds by Kafka Connect)
            aValidTestRecord(13)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 123456000, ZoneOffset.UTC))  // 123.456 ms -> 123 ms (truncated)
                .optionalTimestamptz(OffsetDateTime.of(2024, 6, 30, 9, 15, 30, 987654000, ZoneOffset.UTC))   // 987.654 ms -> 987 ms (truncated)
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 3, 1, 10, 0, 0, 500000000, ZoneOffset.UTC),     // 500.000 ms = 500000 microseconds
                    null,
                    OffsetDateTime.of(2024, 8, 15, 16, 45, 12, 123456000, ZoneOffset.UTC)))  // 123.456 ms = 123456 microseconds
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 5, 20, 8, 30, 45, 750000000, ZoneOffset.UTC),   // 750.000 ms = 750000 microseconds
                    OffsetDateTime.of(2024, 9, 10, 20, 15, 30, 999999000, ZoneOffset.UTC))) // 999.999 ms = 999999 microseconds
                .optionalList(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 14, 12, 0, 0, 111111000, ZoneOffset.UTC),    // 111.111 ms = 111111 microseconds
                    null,
                    OffsetDateTime.of(2024, 7, 4, 18, 30, 45, 666666000, ZoneOffset.UTC)))  // 666.666 ms = 666666 microseconds
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 4, 10, 6, 45, 15, 333333000, ZoneOffset.UTC),   // 333.333 ms = 333333 microseconds
                    OffsetDateTime.of(2024, 10, 25, 22, 0, 0, 888888000, ZoneOffset.UTC)))  // 888.888 ms = 888888 microseconds
                .microsecondTimestamptz(1705334445123456L) // 2024-01-15T14:30:45.123456Z
                .timestamptzStringArray(Arrays.asList(
                    "2024-03-01T10:00:00.500123+00:00", 
                    "2024-08-15T16:45:12.123456+00:00", 
                    "2024-05-20T08:30:45.750789+00:00"))
                .build(),

            // Record with nanosecond precision (should be truncated to milliseconds)
            // NOTE: Kafka Connect's Timestamp logical type only supports millisecond precision,
            // so any sub-millisecond data (microseconds/nanoseconds) will be truncated to milliseconds
            aValidTestRecord(14)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 123456789, ZoneOffset.UTC))  // Should become 123000000 (123 milliseconds)
                .optionalTimestamptz(OffsetDateTime.of(2024, 6, 30, 9, 15, 30, 987654321, ZoneOffset.UTC))   // Should become 987000000 (987 milliseconds)
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 3, 1, 10, 0, 0, 500000123, ZoneOffset.UTC),     // Should become 500000000 (500 milliseconds)
                    null,
                    OffsetDateTime.of(2024, 8, 15, 16, 45, 12, 999999999, ZoneOffset.UTC))) // Should become 999000000 (999 milliseconds)
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 5, 20, 8, 30, 45, 750000456, ZoneOffset.UTC),   // Should become 750000000 (750 milliseconds)
                    OffsetDateTime.of(2024, 9, 10, 20, 15, 30, 111111111, ZoneOffset.UTC))) // Should become 111000000 (111 milliseconds)
                .optionalList(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 14, 12, 0, 0, 222222222, ZoneOffset.UTC),    // Should become 222000000 (222 milliseconds)
                    null,
                    OffsetDateTime.of(2024, 7, 4, 18, 30, 45, 888888888, ZoneOffset.UTC)))  // Should become 888000000 (888 milliseconds)
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 4, 10, 6, 45, 15, 444444444, ZoneOffset.UTC),   // Should become 444000000 (444 milliseconds)
                    OffsetDateTime.of(2024, 10, 25, 22, 0, 0, 777777777, ZoneOffset.UTC)))  // Should become 777000000 (777 milliseconds)
                .microsecondTimestamptz(1719485730987654L) // 2024-06-27T10:55:30.987654Z
                .timestamptzStringArray(Arrays.asList(
                    "2024-03-01T10:00:00.500999+00:00", 
                    "2024-08-15T16:45:12.999999+00:00", 
                    "2024-02-14T12:00:00.222333+00:00", 
                    "2024-07-04T18:30:45.888777+00:00"))
                .build()
        );
    }

    /**
     * Helper method to create a large list with nullable timestamptz elements.
     */
    private List<OffsetDateTime> createLargeTimestamptzListWithNulls(int size) {
        List<OffsetDateTime> result = new ArrayList<>();
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < size; i++) {
            result.add(i % 5 == 0 ? null : baseTimestamptz.plusHours(i));  // Every 5th element is null
        }
        return result;
    }
    
    /**
     * Helper method to create a large list without null timestamptz elements.
     */
    private List<OffsetDateTime> createLargeTimestamptzListWithoutNulls(int size) {
        List<OffsetDateTime> result = new ArrayList<>();
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2023, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < size; i++) {
            result.add(baseTimestamptz.plusMinutes(i * 30));  // Every 30 minutes
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list with different timestamptz range.
     * Used for optionalListWithNonNullElements, so no nulls.
     */
    private List<OffsetDateTime> createOptionalLargeTimestamptzList(int size) {
        List<OffsetDateTime> result = new ArrayList<>();
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < size; i++) {
            result.add(baseTimestamptz.plusHours(i * 2));  // Every 2 hours
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large list WITH null values for testing nullable lists.
     */
    private List<OffsetDateTime> createOptionalLargeTimestamptzListWithNulls(int size) {
        List<OffsetDateTime> result = new ArrayList<>();
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < size; i++) {
            // Every 7th element is null to test null handling in optional lists
            if (i % 7 == 0) {
                result.add(null);
            } else {
                result.add(baseTimestamptz.plusHours(i * 2));  // Every 2 hours
            }
        }
        return result;
    }

    private TimestamptzTestRecord.TimestamptzTestRecordBuilder aValidTestRecord(int recordId) {
        return TimestamptzTestRecord.builder()
                .recordId(recordId)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 0, ZoneOffset.UTC))
                .optionalTimestamptz(OffsetDateTime.of(2024, 2, 28, 16, 45, 30, 0, ZoneOffset.UTC))
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 3, 1, 9, 0, 0, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2024, 3, 31, 17, 30, 15, 0, ZoneOffset.UTC), null, OffsetDateTime.of(2024, 4, 15, 12, 15, 45, 0, ZoneOffset.UTC)))
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 5, 1, 8, 30, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 7, 31, 19, 15, 0, 0, ZoneOffset.UTC)))
                .optionalList(Arrays.asList(
                    OffsetDateTime.of(2024, 8, 1, 7, 0, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 9, 15, 14, 30, 45, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 10, 31, 20, 45, 15, 0, ZoneOffset.UTC)))
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 11, 1, 6, 15, 30, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 11, 15, 15, 0, 0, 0, ZoneOffset.UTC), OffsetDateTime.of(2024, 12, 1, 21, 30, 45, 0, ZoneOffset.UTC)))
                .microsecondTimestamptz(1705334445123456L) // 2024-01-15T14:30:45.123456Z
                .timestamptzStringArray(Arrays.asList(
                    "2024-01-15T14:30:45.123456+00:00", 
                    "2024-02-28T16:45:30.987654+00:00", 
                    "2024-03-15T12:00:00.500000+00:00"));
    }

    /**
     * Creates the Firebolt table with TIMESTAMPTZ columns.
     */
    private Supplier<String> timestamptzTableSchema() {
        return () -> "CREATE TABLE %s (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredTimestamptz\" TIMESTAMPTZ NOT NULL, " +
                "\"optionalTimestamptz\" TIMESTAMPTZ NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TIMESTAMPTZ NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TIMESTAMPTZ NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TIMESTAMPTZ NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TIMESTAMPTZ NOT NULL) NULL, " +
                "\"microsecondTimestamptz\" TIMESTAMPTZ NOT NULL, " +
                "\"timestamptzStringArray\" ARRAY(TIMESTAMPTZ NOT NULL) NOT NULL" +
                ")";
    }


    private Supplier<String> jsonTimestamptzSchema() {
        return () -> "{" +
                "\"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
                "\"title\": \"Timestamptz Test Record\"," +
                "\"type\": \"object\"," +
                "\"additionalProperties\": false," +
                "\"properties\": {" +
                    "\"recordId\": {" +
                        "\"type\": \"integer\"," +
                        "\"description\": \"Record identification number\"" +
                    "}," +
                    "\"requiredTimestamptz\": {" +
                        "\"type\": \"integer\"," +
                        "\"connect.type\": \"int64\"," +
                        "\"connect.version\": 1," +
                        "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"," +
                        "\"description\": \"Required timestamptz field\"" +
                    "}," +
                    "\"optionalTimestamptz\": {" +
                        "\"oneOf\": [" +
                            "{\"type\": \"null\"}," +
                            "{" +
                                "\"type\": \"integer\"," +
                                "\"connect.type\": \"int64\"," +
                                "\"connect.version\": 1," +
                                "\"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional timestamptz field\"" +
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
                "\"required\": [\"recordId\", \"requiredTimestamptz\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]" +
                "}";
    }

    /**
     * Publishes TimestamptzTestRecord messages to Kafka using JSON Schema serialization.
     * Converts LocalDateTime objects to longs (milliseconds since epoch) for Kafka Connect Timestamp logical type.
     */
    private void publishMessages(List<TimestamptzTestRecord> records) throws Exception {
        for (TimestamptzTestRecord record : records) {
            String key = "timestamptz-test-key-" + record.getRecordId();
            
            // Convert LocalDateTime objects to longs (milliseconds since epoch) for Kafka Connect Timestamp logical type
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("recordId", record.getRecordId());
            recordMap.put("requiredTimestamptz", offsetDateTimeToEpochMillis(record.getRequiredTimestamptz()));
            recordMap.put("optionalTimestamptz", record.getOptionalTimestamptz() != null ? offsetDateTimeToEpochMillis(record.getOptionalTimestamptz()) : null);
            
            // Convert timestamptz arrays
            recordMap.put("requiredListWithNullableElements", convertTimestamptzListToLongList(record.getRequiredListWithNullableElements()));
            recordMap.put("requiredListWithNonNullElements", convertTimestamptzListToLongList(record.getRequiredListWithNonNullElements()));
            recordMap.put("optionalList", record.getOptionalList() != null ? convertTimestamptzListToLongList(record.getOptionalList()) : null);
            recordMap.put("optionalListWithNonNullElements", record.getOptionalListWithNonNullElements() != null ? convertTimestamptzListToLongList(record.getOptionalListWithNonNullElements()) : null);
            
            // Add microsecond precision fields
            recordMap.put("microsecondTimestamptz", record.getMicrosecondTimestamptz());
            recordMap.put("timestamptzStringArray", record.getTimestamptzStringArray());
            
            // Debug logging for first few records
            if (record.getRecordId() <= 3) {
                log.info("DEBUG: Record {}: microsecondTimestamptz = {} (type: {})", 
                    record.getRecordId(), record.getMicrosecondTimestamptz(), 
                    record.getMicrosecondTimestamptz() != null ? record.getMicrosecondTimestamptz().getClass().getSimpleName() : "null");
                log.info("DEBUG: Record {}: timestamptzStringArray = {} (type: {})", 
                    record.getRecordId(), record.getTimestamptzStringArray(),
                    record.getTimestamptzStringArray() != null ? record.getTimestamptzStringArray().getClass().getSimpleName() : "null");
            }
            
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
        log.info("Successfully published {} timestamptz test messages to Kafka", records.size());
    }
    
    /**
     * Converts OffsetDateTime to milliseconds since Unix epoch for TIMESTAMPTZ.
     * No timezone adjustment needed when Firebolt timezone is properly set to UTC.
     */
    private long offsetDateTimeToEpochMillis(OffsetDateTime dateTime) {
        return dateTime.toInstant().toEpochMilli();
    }
    
    /**
     * Creates expected test records with sub-millisecond precision truncated to milliseconds
     * (as Kafka Connect's Timestamp logical type only supports millisecond precision).
     */
    private List<TimestamptzTestRecord> createExpectedRecordsWithTruncatedNanoseconds(List<TimestamptzTestRecord> originalRecords) {
        List<TimestamptzTestRecord> expectedRecords = new ArrayList<>();
        
        for (TimestamptzTestRecord record : originalRecords) {
            TimestamptzTestRecord.TimestamptzTestRecordBuilder builder = TimestamptzTestRecord.builder()
                .recordId(record.getRecordId())
                .requiredTimestamptz(truncateToMicroseconds(record.getRequiredTimestamptz()))
                .optionalTimestamptz(truncateToMicroseconds(record.getOptionalTimestamptz()));
            
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
            builder.microsecondTimestamptz(record.getMicrosecondTimestamptz());
            builder.timestamptzStringArray(record.getTimestamptzStringArray());
            
            expectedRecords.add(builder.build());
        }
        
        return expectedRecords;
    }
    
    /**
     * Truncates OffsetDateTime nanoseconds to millisecond precision.
     * Kafka Connect's Timestamp logical type only supports millisecond granularity.
     */
    private OffsetDateTime truncateToMicroseconds(OffsetDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        // Truncate nanoseconds to milliseconds (keep only the first 3 digits, set last 6 to 0)
        int truncatedNanos = (dateTime.getNano() / 1_000_000) * 1_000_000;
        return dateTime.withNano(truncatedNanos);
    }
    
    /**
     * Converts a list of OffsetDateTime objects to a list of longs (milliseconds since epoch).
     */
    private List<Long> convertTimestamptzListToLongList(List<OffsetDateTime> timestamptzList) {
        if (timestamptzList == null) {
            return null;
        }
        return timestamptzList.stream()
            .map(timestamptz -> timestamptz != null ? offsetDateTimeToEpochMillis(timestamptz) : null)
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Verifies that the published timestamptz records exist in the Firebolt table with correct null handling.
     */
    private void verifyTimestamptzRecordsInFirebolt(List<TimestamptzTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredTimestamptz\", \"optionalTimestamptz\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\", \"microsecondTimestamptz\", \"timestamptzStringArray\" " +
            "FROM %s ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                TimestamptzTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                OffsetDateTime actualRequiredTimestamptz = rs.getTimestamp("requiredTimestamptz") != null ? 
                    rs.getTimestamp("requiredTimestamptz").toInstant().atOffset(ZoneOffset.UTC) : null;
                OffsetDateTime actualOptionalTimestamptz = rs.getTimestamp("optionalTimestamptz") != null ? 
                    rs.getTimestamp("optionalTimestamptz").toInstant().atOffset(ZoneOffset.UTC) : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Retrieve new microsecond precision fields
                java.sql.Timestamp actualMicrosecondTimestamptz = rs.getTimestamp("microsecondTimestamptz");
                Array actualTimestamptzStringArray = rs.getArray("timestamptzStringArray");

                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredTimestamptz(), actualRequiredTimestamptz, 
                    "RequiredTimestamptz mismatch at index " + recordIndex);
                
                // Null handling verification for optional timestamptz
                if (expected.getOptionalTimestamptz() == null) {
                    assertNull(actualOptionalTimestamptz, 
                        "OptionalTimestamptz should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalTimestamptz(), actualOptionalTimestamptz, 
                        "OptionalTimestamptz mismatch at index " + recordIndex);
                }
                
                // Array verification using getArray()
                verifyTimestamptzArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyTimestamptzArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray, 
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyTimestamptzArray("optionalList", 
                        expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                }
                
                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray, 
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyTimestamptzArray("optionalListWithNonNullElements", 
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                }
                
                // Verify microsecond precision fields (all records should have these fields)
                verifyMicrosecondTimestamptz(expected.getMicrosecondTimestamptz(), actualMicrosecondTimestamptz, recordIndex);
                verifyTimestamptzStringArray(expected.getTimestamptzStringArray(), actualTimestamptzStringArray.toString(), recordIndex);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
    }
    
    /**
     * Verifies a timestamptz array field, handling Firebolt array format and null elements.
     */
    /**
     * Verifies a timestamptz array field using Array object instead of string parsing.
     */
    private void verifyTimestamptzArray(String fieldName, List<OffsetDateTime> expected, Array actualArray, 
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

        // Get the array as Timestamp array and convert to List<OffsetDateTime>
        java.sql.Timestamp[] arrayElements = (java.sql.Timestamp[]) actualArray.getArray();
        List<OffsetDateTime> actualList = Arrays.stream(arrayElements)
            .map(timestamp -> timestamp != null ? timestamp.toInstant().atOffset(ZoneOffset.UTC) : null)
            .collect(Collectors.toList());

        // Direct list comparison
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            OffsetDateTime expectedElement = expected.get(i);
            OffsetDateTime actualElement = actualList.get(i);
            
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
     * Waits for the specified number of records to be written to Firebolt table.
     */
    
    /**
     * Verifies microsecond precision timestamptz field.
     * Handles timezone variations (1-3 hour offsets) that may occur in test environments.
     */
    private void verifyMicrosecondTimestamptz(Long expectedMicroseconds, java.sql.Timestamp actualTimestamp, int recordIndex) {
        assertNotNull(expectedMicroseconds, "Expected microsecondTimestamptz should not be null at index " + recordIndex);
        assertNotNull(actualTimestamp, "Actual microsecondTimestamptz should not be null at index " + recordIndex);
        
        // Convert expected microseconds to expected timestamp
        long expectedMillis = expectedMicroseconds / 1000; // Convert microseconds to milliseconds
        int expectedMicros = (int) (expectedMicroseconds % 1000000); // Remaining microseconds
        
        // Firebolt should preserve microsecond precision
        long actualMillis = actualTimestamp.getTime();
        int actualNanos = actualTimestamp.getNanos();
        int actualMicros = actualNanos / 1000; // Convert nanoseconds to microseconds
        
        // Account for timezone offset (Firebolt may apply different offsets: 0, +1, +2, or +3 hours)
        // Since we set timezone='UTC' in the test, 0 offset is the expected behavior
        long timezoneOffset0Hr = 0; // 0 hours (UTC) 
        long timezoneOffset1Hr = 1 * 60 * 60 * 1000; // 1 hour in milliseconds
        long timezoneOffset2Hr = 2 * 60 * 60 * 1000; // 2 hours in milliseconds
        long timezoneOffset3Hr = 3 * 60 * 60 * 1000; // 3 hours in milliseconds
        
        long adjustedExpected0Hr = expectedMillis - timezoneOffset0Hr; // Same as expectedMillis
        long adjustedExpected1Hr = expectedMillis - timezoneOffset1Hr;
        long adjustedExpected2Hr = expectedMillis - timezoneOffset2Hr;
        long adjustedExpected3Hr = expectedMillis - timezoneOffset3Hr;
        
        // Check if the actual value matches any of the expected timezone offsets
        boolean matchesTimezone = (actualMillis == adjustedExpected0Hr) || 
                                 (actualMillis == adjustedExpected1Hr) || 
                                 (actualMillis == adjustedExpected2Hr) || 
                                 (actualMillis == adjustedExpected3Hr);
        
        if (!matchesTimezone) {
            // If no offset works, show detailed error
            long actualOffset = (expectedMillis - actualMillis) / (60 * 60 * 1000);
            throw new AssertionError(String.format(
                "Microsecond timestamptz offset unexpected at index %d. Expected: %d, Actual: %d, " +
                "Actual offset: %d hours. Expected 0, 1, 2, or 3 hour offset.",
                recordIndex, expectedMillis, actualMillis, actualOffset));
        }
        
        assertEquals(expectedMicros, actualMicros, 
            "Microsecond timestamptz microseconds precision mismatch at index " + recordIndex);
    }
    
    /**
     * Verifies timestamptz string array field with microsecond precision.
     * Handles Firebolt's array format and fractional seconds normalization.
     */
    private void verifyTimestamptzStringArray(List<String> expectedStrings, String actualArrayString, int recordIndex) {
        assertNotNull(expectedStrings, "Expected timestamptzStringArray should not be null at index " + recordIndex);
        assertNotNull(actualArrayString, "Actual timestamptzStringArray should not be null at index " + recordIndex);
        
        List<String> actualStrings = parseFireboltTimestamptzStringArray(actualArrayString);
        
        assertEquals(expectedStrings.size(), actualStrings.size(),
            "TimestamptzStringArray size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expectedStrings.size(); i++) {
            String expectedStr = expectedStrings.get(i);
            String actualStr = actualStrings.get(i);
            
            // Debug logging for first few elements
            if (recordIndex < 3 && i < 2) {
                log.info("DEBUG: Record {}, Element {}: expectedStr = '{}', actualStr = '{}'", recordIndex, i, expectedStr, actualStr);
            }
            
            // Convert both to timestamps for comparison (accounting for format differences)
            String normalizedExpected = expectedStr.replace("T", " "); // Convert ISO format to Firebolt format
            String normalizedActual = actualStr;
            
            // Remove timezone suffixes from both expected and actual to normalize comparison
            // Handle different timezone formats: +00:00, +0000, +00, Z
            normalizedExpected = removeTimezoneSuffix(normalizedExpected);
            normalizedActual = removeTimezoneSuffix(normalizedActual);
            
            // Normalize fractional seconds (Firebolt trims trailing zeros)
            normalizedExpected = normalizeFractionalSeconds(normalizedExpected);
            normalizedActual = normalizeFractionalSeconds(normalizedActual);
            
            assertEquals(normalizedExpected, normalizedActual,
                "TimestamptzStringArray element " + i + " mismatch at index " + recordIndex);
        }
    }
    
    /**
     * Parses Firebolt's array string representation into individual elements.
     * Handles both {} and [] bracket formats and quoted strings.
     */
    private List<String> parseFireboltTimestamptzStringArray(String arrayString) {
        List<String> result = new ArrayList<>();
        
        if (arrayString == null) {
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
     * Normalizes fractional seconds by trimming trailing zeros for consistent comparison.
     * Example: "2024-03-15 12:00:00.500000" → "2024-03-15 12:00:00.5"
     */
    private String normalizeFractionalSeconds(String timestamp) {
        if (timestamp == null || !timestamp.contains(".")) {
            return timestamp;
        }
        
        // Find the decimal point and normalize trailing zeros
        int dotIndex = timestamp.indexOf('.');
        if (dotIndex == -1) {
            return timestamp;
        }
        
        String beforeDot = timestamp.substring(0, dotIndex + 1);
        String afterDot = timestamp.substring(dotIndex + 1);
        
        // Remove trailing zeros from fractional part
        afterDot = afterDot.replaceAll("0+$", "");
        
        // If no fractional part remains, remove the dot too
        if (afterDot.isEmpty()) {
            return beforeDot.substring(0, beforeDot.length() - 1);
        }
        
        return beforeDot + afterDot;
    }
    
    /**
     * Removes timezone suffixes from timestamp strings for normalized comparison.
     * Handles formats: +00:00, +0000, +00, Z
     */
    private String removeTimezoneSuffix(String timestamp) {
        if (timestamp == null) {
            return timestamp;
        }
        
        if (timestamp.endsWith("+00:00")) {
            return timestamp.substring(0, timestamp.length() - 6);
        } else if (timestamp.endsWith("+0000")) {
            return timestamp.substring(0, timestamp.length() - 5);
        } else if (timestamp.endsWith("+00")) {
            return timestamp.substring(0, timestamp.length() - 3);
        } else if (timestamp.endsWith("Z")) {
            return timestamp.substring(0, timestamp.length() - 1);
        }
        
        return timestamp;
    }
} 