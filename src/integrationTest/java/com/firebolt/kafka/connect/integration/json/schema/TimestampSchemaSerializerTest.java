package com.firebolt.kafka.connect.integration.json.schema;

import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.TimestampTestRecord;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
public class TimestampSchemaSerializerTest extends SchemaBaseIntegrationTest {

    private static final String TOPIC_NAME = generateTopicName("timestamp-test-topic");
    private static final String TABLE_NAME = generateTableName("timestamp_test_table");
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, Object> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("timestamp-serializer-test");
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

    // When we have a way to run firebolt-core with the image that has the fix we can uncomment and use sqlAndBinaryTestSetupWithOrWithoutNulls
    // until then we will run these tests locally against core
    @ParameterizedTest
//    @MethodSource("sqlAndBinaryTestSetupWithOrWithoutNulls")
    @MethodSource("sqlIngestionTypeWithOrWithoutNulls")
    void testTimestampSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for timestamp data type", testDescription);

        // Setup test resources using centralized method
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                timestampTableSchema(), jsonTimestampSchema(), connectorOverrides);

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
                .microsecondTimestampList(Arrays.asList(
                    1709204400500123L, // 2024-03-01T10:00:00.500123
                    1723737912123456L, // 2024-08-15T16:45:12.123456
                    1716138045750789L  // 2024-05-20T08:30:45.750789
                ))
                .timestampString("2024-07-04T12:30:15.987654")   // Independence Day with precision
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
                .microsecondTimestampList(Arrays.asList(
                    1709204400500999L, // 2024-03-01T10:00:00.500999
                    1723737912999999L, // 2024-08-15T16:45:12.999999
                    1707912000222333L, // 2024-02-14T12:00:00.222333
                    1720116645888777L  // 2024-07-04T18:30:45.888777
                ))
                .timestampString("2024-12-25T00:00:00.121212")   // Christmas with palindromic microseconds
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
                .timestampAsString(LocalDateTime.of(2024, 1, 15, 14, 30, 45))
                .timestampListAsString(Arrays.asList(
                        LocalDateTime.of(2024, 1, 15, 14, 30, 45),
                        LocalDateTime.of(2024, 1, 16, 14, 30, 45)
                ))
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
                .microsecondTimestamp(1705334445123456L)
                .timestampStringArray(Arrays.asList(
                    "2024-01-15T14:30:45.123456",
                    "2024-02-28T16:45:30.987654",
                    "2024-03-15T12:00:00.500000"))
                .microsecondTimestampList(Arrays.asList(
                    1705334445123456L,
                    1709139930987654L,
                    1710504000500000L
                ))
                .timestampString("2024-04-01T08:15:30.111222");
    }
    
    /**
     * Creates the Firebolt table with TIMESTAMP columns.
     */
    private Supplier<String> timestampTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredTimestamp\" TIMESTAMP NOT NULL, " +
                "\"timestampAsString\" TIMESTAMP NOT NULL, " +
                "\"optionalTimestamp\" TIMESTAMP NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TIMESTAMP NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TIMESTAMP NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TIMESTAMP NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TIMESTAMP NOT NULL) NULL, " +
                "\"microsecondTimestamp\" TIMESTAMP  NULL, " +
                "\"timestampStringArray\" ARRAY(TIMESTAMP NOT NULL)  NULL, " +
                "\"microsecondTimestampList\" ARRAY(TIMESTAMP NOT NULL)  NULL, " +
                "\"timestampListAsString\" ARRAY(TIMESTAMP NOT NULL)  NOT NULL, " +
                "\"timestampString\" TIMESTAMP  NULL" +
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
                        "\"connect.type\": \"int32\",\n" +
                        "\"description\": \"Record identification number\"" +
                    "}," +
                    "\"requiredTimestamp\": {" +
                        "\"type\": \"integer\"," +
                        "\"connect.type\": \"int64\"," +
                        "\"title\": \"org.apache.kafka.connect.data.Timestamp\"," +
                        "\"description\": \"Required timestamp field\"" +
                    "}," +
                    "\"timestampAsString\": {" +
                        "\"type\": \"string\"," +
                        "\"format\": \"date-time\"," +
                        "\"description\": \"Required LocalDateTime serialized as string\"" +
                    "}," +
                    "\"optionalTimestamp\": {" +
                        "\"oneOf\": [" +
                            "{\"type\": \"null\"}," +
                            "{" +
                                "\"type\": \"integer\"," +
                                "\"connect.type\": \"int64\"," +
                                "\"title\": \"org.apache.kafka.connect.data.Timestamp\"" +
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
                                    "\"title\": \"org.apache.kafka.connect.data.Timestamp\"" +
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
                            "\"title\": \"org.apache.kafka.connect.data.Timestamp\"" +
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
                                            "\"title\": \"org.apache.kafka.connect.data.Timestamp\"" +
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
                                    "\"title\": \"org.apache.kafka.connect.data.Timestamp\"" +
                                "}" +
                            "}" +
                        "]," +
                        "\"description\": \"Optional list with non-null elements\"" +
                    "}," +
                    "\"microsecondTimestamp\": {" +
                        "\"type\": \"integer\"," +
                        "\"connect.type\": \"int64\"," +
                        "\"description\": \"Timestamp with microsecond precision (microseconds since epoch)\"" +
                    "}," +
                    "\"timestampStringArray\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"type\": \"string\"," +
                            "\"format\": \"date-time\"" +
                        "}" +
                    "}," +
                    "\"microsecondTimestampList\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"type\": \"integer\"," +
                            "\"connect.type\": \"int64\"" +
                        "}," +
                        "\"description\": \"Array of microsecond precision timestamps (microseconds since epoch)\"" +
                    "}," +
                    "\"timestampListAsString\": {" +
                        "\"type\": \"array\"," +
                        "\"items\": {" +
                            "\"type\": \"string\"," +
                            "\"format\": \"date-time\"" +
                        "}" +
                    "}," +
                    "\"timestampString\": {" +
                        "\"type\": \"string\"," +
                        "\"format\": \"date-time\"," +
                        "\"description\": \"Single timestamp string with microsecond precision\"" +
                    "}" +
                "}," +
                "\"required\": [\"recordId\", \"requiredTimestamp\", \"timestampAsString\", \"timestampListAsString\", \"requiredListWithNullableElements\", \"requiredListWithNonNullElements\"]" +
                "}";
    }
    
    /**
     * Publishes TimestampTestRecord messages to Kafka using JSON Schema serialization.
     * Converts LocalDateTime objects to longs (milliseconds since epoch) for Kafka Connect Timestamp logical type.
     */
    private void publishMessages(List<TimestampTestRecord> records) throws Exception {
        for (TimestampTestRecord record : records) {
            String key = "timestamp-test-key-" + record.getRecordId();
            
            ProducerRecord<String, Object> producerRecord =
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
     * Converts Date to milliseconds since Unix epoch (1970-01-01T00:00:00Z).
     */
    private long localDateTimeToEpochMillis(Date date) {
        return date.toInstant().toEpochMilli();
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
                .requiredTimestamp(truncateToMilliseconds(record.getRequiredTimestamp()))
                .optionalTimestamp(truncateToMilliseconds(record.getOptionalTimestamp()));
            
            // Handle arrays with potential null elements
            if (record.getRequiredListWithNullableElements() != null) {
                builder.requiredListWithNullableElements(
                    record.getRequiredListWithNullableElements().stream()
                        .map(this::truncateToMilliseconds)
                        .collect(Collectors.toList()));
            }
            
            if (record.getRequiredListWithNonNullElements() != null) {
                builder.requiredListWithNonNullElements(
                    record.getRequiredListWithNonNullElements().stream()
                        .map(this::truncateToMilliseconds)
                        .collect(Collectors.toList()));
            }
            
            if (record.getOptionalList() != null) {
                builder.optionalList(
                    record.getOptionalList().stream()
                        .map(this::truncateToMilliseconds)
                        .collect(Collectors.toList()));
            }
            
            if (record.getOptionalListWithNonNullElements() != null) {
                builder.optionalListWithNonNullElements(
                    record.getOptionalListWithNonNullElements().stream()
                        .map(this::truncateToMilliseconds)
                        .collect(Collectors.toList()));
            }
            
            // Add the new microsecond precision fields (these should be preserved as-is)
            builder.microsecondTimestamp(record.getMicrosecondTimestamp());
            builder.timestampStringArray(record.getTimestampStringArray());
            builder.microsecondTimestampList(record.getMicrosecondTimestampList());
            builder.timestampString(record.getTimestampString());
            builder.timestampAsString(record.getTimestampAsString());
            builder.timestampListAsString(record.getTimestampListAsString());

            expectedRecords.add(builder.build());
        }
        
        return expectedRecords;
    }
    
    /**
     * Truncates Date milliseconds to millisecond precision (no-op but keeps API parallel to previous).
     */
    private LocalDateTime truncateToMilliseconds(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        int truncatedNanos = (dateTime.getNano() / 1_000_000) * 1_000_000;
        return dateTime.withNano(truncatedNanos);
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
            "SELECT \"recordId\", \"requiredTimestamp\", \"timestampAsString\", \"optionalTimestamp\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\", \"microsecondTimestamp\", \"timestampStringArray\", " +
            "\"microsecondTimestampList\", \"timestampListAsString\", \"timestampString\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);

        // in order to get the timestamp in the current timezone we need to use a Calendar instance
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                TimestampTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");

                LocalDateTime actualRequiredTimestamp = rs.getTimestamp("requiredTimestamp") != null ? rs.getTimestamp("requiredTimestamp").toLocalDateTime() : null;
                LocalDateTime actualTimestampAsString = rs.getTimestamp("timestampAsString") != null ? rs.getTimestamp("timestampAsString").toLocalDateTime() : null;
                LocalDateTime actualOptionalTimestamp = rs.getTimestamp("optionalTimestamp") != null ? rs.getTimestamp("optionalTimestamp").toLocalDateTime() : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Retrieve new microsecond precision fields
                java.sql.Timestamp actualMicrosecondTimestamp = rs.getTimestamp("microsecondTimestamp");
                Array actualTimestampArray = rs.getArray("timestampStringArray");
                Array actualMicrosecondTimestampListArray = rs.getArray("microsecondTimestampList");
                Array actualTimestampListAsStringArray = rs.getArray("timestampListAsString");
                java.sql.Timestamp actualTimestampString = rs.getTimestamp("timestampString");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredTimestamp(), actualRequiredTimestamp,
                    "RequiredTimestamp mismatch at index " + recordIndex);
                assertEquals(expected.getTimestampAsString(), actualTimestampAsString, 
                    "timestampAsString mismatch at index " + recordIndex);
                
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
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex);
                    
                verifyTimestampArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex);
                
                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray, 
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyTimestampArray("optionalList", 
                        expected.getOptionalList(), actualOptionalListArray, recordIndex);
                }
                
                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray, 
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyTimestampArray("optionalListWithNonNullElements", 
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex);
                }
                
                // Verify microsecond precision fields (all records should have these fields)
                verifyMicrosecondTimestamp(expected.getMicrosecondTimestamp(), actualMicrosecondTimestamp, recordIndex);
                verifyTimestampStringArray(expected.getTimestampStringArray(), actualTimestampArray, recordIndex);
                verifyMicrosecondTimestampListArray(expected.getMicrosecondTimestampList(), actualMicrosecondTimestampListArray, recordIndex);
                verifyTimestampString(expected.getTimestampString(), actualTimestampString, recordIndex);
                
                // timestampListAsString verification (mandatory)
                verifyTimestampArray("timestampListAsString",
                    expected.getTimestampListAsString(), actualTimestampListAsStringArray, recordIndex);
                
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
                                      int recordIndex) throws SQLException {
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
            .map(ts -> ts != null ? ts.toLocalDateTime() : null)
            .collect(Collectors.toList());

        // Direct list comparison
       assertEqualLocalDateTime(expected, actualList, recordIndex);
    }
    
    /**
     * Verifies microsecond precision timestamp field by comparing the expected Long (microseconds since epoch)
     * with the actual Timestamp retrieved from Firebolt.
     */
    private void verifyMicrosecondTimestamp(Long expectedMicroseconds, java.sql.Timestamp actualTimestamp, int recordIndex) {
        assertNotNull(expectedMicroseconds, "Expected microsecondTimestamp should not be null at index " + recordIndex);
        assertNotNull(actualTimestamp, "Actual microsecondTimestamp should not be null at index " + recordIndex);
        assertEquals(fromMicros(expectedMicroseconds), actualTimestamp.toInstant());
    }

    // Convert expected microseconds to expected timestamp. Firebolt stores the data in UTC. We need to substract the default timezone of the test machine
    private Instant fromMicros(long micros) {
        Timestamp timestamp = asTimestamp(micros);

        LocalDateTime ldt = timestamp.toLocalDateTime();

        ZoneId zone = ZoneId.of(java.util.TimeZone.getDefault().getID());
        ZonedDateTime zdt = ldt.atZone(zone);
        ZoneOffset offset = zdt.getOffset();
        return timestamp.toInstant().minus(offset.getTotalSeconds(), ChronoUnit.SECONDS);
    }

    /**
     * Verifies microsecond timestamp array by comparing expected Long array (microseconds since epoch)
     * with actual Timestamp array retrieved from Firebolt.
     */
    private void verifyMicrosecondTimestampListArray(List<Long> expectedMicroseconds, Array actualArray, int recordIndex) throws SQLException {
        assertNotNull(expectedMicroseconds, "Expected microsecondTimestampList should not be null at index " + recordIndex);
        assertNotNull(actualArray, "Actual microsecondTimestampList should not be null at index " + recordIndex);
        
        // Check that the array base type is TIMESTAMP (Types.TIMESTAMP = 93)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.TIMESTAMP, baseType);

        // Get the array as Timestamp array and convert to List<Long> (microseconds since epoch)
        java.sql.Timestamp[] arrayElements = (java.sql.Timestamp[]) actualArray.getArray();
        assertEquals(expectedMicroseconds.size(), arrayElements.length,
            "MicrosecondTimestampList size mismatch at index " + recordIndex);

        for (int i = 0; i < expectedMicroseconds.size(); i++) {
            Long expectedElement = expectedMicroseconds.get(i);
            assertEquals(fromMicros(expectedElement), arrayElements[i].toInstant());
        }
    }

    /**
     * Verifies timestamp string array by parsing both expected and actual string arrays
     * and comparing their timestamp values.
     */
    private void verifyTimestampStringArray(List<String> expectedStrings, Array actualArray, int recordIndex) throws SQLException {
        assertNotNull(expectedStrings, "Expected timestampStringArray should not be null at index " + recordIndex);
        assertNotNull(actualArray, "Actual timestampStringArray should not be null at index " + recordIndex);

        // Check that the array base type is TIMESTAMP (Types.TIMESTAMP = 93)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.TIMESTAMP, baseType);

        // Get the array as Timestamp array and convert to List<Date>
        java.sql.Timestamp[] arrayElements = (java.sql.Timestamp[]) actualArray.getArray();
        List<LocalDateTime> actualList = Arrays.stream(arrayElements)
                .map(ts -> ts != null ? ts.toLocalDateTime() : null)
                .collect(Collectors.toList());

        List<LocalDateTime> expectedList = expectedStrings.stream()
                .map(LocalDateTime::parse)
                .collect(Collectors.toList());
        assertEqualLocalDateTime(expectedList, actualList, recordIndex);
    }

    /**
     * Verifies single timestamp string by parsing expected string and comparing with actual Timestamp.
     */
    private void verifyTimestampString(String expectedString, java.sql.Timestamp actualTimestamp, int recordIndex) {
        assertNotNull(expectedString, "Expected timestampString should not be null at index " + recordIndex);
        assertNotNull(actualTimestamp, "Actual timestampString should not be null at index " + recordIndex);
        
        // Parse the expected string and compare Dates in UTC
        LocalDateTime expectedLocalDateTime = LocalDateTime.parse(expectedString);
        LocalDateTime actualLocalDateTime = actualTimestamp.toLocalDateTime();

        assertEquals(expectedLocalDateTime, actualLocalDateTime,
            "TimestampString mismatch at index " + recordIndex);
    }

    private static Timestamp asTimestamp(long micros) {
        long seconds = micros / 1_000_000;
        long microRemainder = micros % 1_000_000;
        Instant instant = Instant.ofEpochSecond(seconds, microRemainder * 1000);
        return Timestamp.from(instant);
    }

    private void assertEqualLocalDateTime(List<LocalDateTime> expected, List<LocalDateTime> actual, int recordIndex) {
        // Direct list comparison
        assertEquals(expected.size(), actual.size());

        for (int i = 0; i < actual.size(); i++) {
            LocalDateTime expectedElement = expected.get(i);
            LocalDateTime actualElement = actual.get(i);

            if (expectedElement == null) {
                assertNull(actualElement,
                        " element " + i + " should be null at index " + recordIndex);
            } else {
                assertEquals(expectedElement, actualElement,
                        " element " + i + " mismatch at index " + recordIndex);
            }
        }
    }

}