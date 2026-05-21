package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.utils.TestTag;

import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.TimestamptzTestRecord;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
public class TimestamptzSchemalessSerializerTest extends SchemalessBaseIntegrationTest {

    private static final String TOPIC_NAME = generateTopicName("timestamptz-test-topic");
    private static final String TABLE_NAME = generateTableName("timestamptz_test_table");
    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("timestamptz-serializer-test");

        // moved setup to test methods
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
    void testTimestamptzSerialization(boolean includeNulls, java.util.Map<String,String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for timestamptz data type (schemaless)", testDescription);

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, timestamptzTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer(includeNulls);
        
        List<TimestamptzTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        // For sub-millisecond precision tests (records 13-14), we need to use truncated expected values
        // since Kafka Connect's Timestamp logical type only supports millisecond precision
        List<TimestamptzTestRecord> expectedRecords = createExpectedRecordsWithTruncatedNanoseconds(testRecords);
        verifyTimestamptzRecordsInFirebolt(expectedRecords);
    }

    @ParameterizedTest
    @MethodSource("ingestionTypes")
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues(java.util.Map<String,String> connectorOverrides) throws Exception {
        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, timestamptzTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer();

        List<TimestamptzTestRecord> mixedRecords = Arrays.asList(
            aValidTestRecord(101)
                .timestamptzString("2024-01-15 14:30:45.123456+02")
                .build(),
            aValidTestRecord(102)
                .timestamptzString("not-a-date") // invalid
                .build(),
            aValidTestRecord(103)
                .timestamptzString("2024-02-28 16:45:30.987654+02")
                .build(),
            aValidTestRecord(104)
                .timestamptzString("2024-02-30 00:00:00Z") // invalid date
                .build()
        );

        publishMessages(mixedRecords);

        // Expect only the 2 valid records to land in Firebolt
        waitForDataInFirebolt(TABLE_NAME, 2);

        // expected timestamps will be in UTC so substract 2hours
        List<TimestamptzTestRecord> expectedValid = Arrays.asList(
            aValidTestRecord(101)
                .timestamptzString("2024-01-15 12:30:45.123456+00")
                .build(),
            aValidTestRecord(103)
                .timestamptzString("2024-02-28 14:45:30.987654+00")
                .build()
        );

        List<TimestamptzTestRecord> expectedTruncated = createExpectedRecordsWithTruncatedNanoseconds(expectedValid);
        verifyTimestamptzRecordsInFirebolt(expectedTruncated);
    }

    /**
     * Creates test records covering all scenarios.
     * NOTE all test records will be created at UTC+2 timezone
     */
    private List<TimestamptzTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with recent timestamptz values
            aValidTestRecord(2)
                .requiredTimestamptz(OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 0, ZoneOffset.ofHours(2)))
                .optionalTimestamptz(OffsetDateTime.of(2025, 1, 1, 0, 0, 2, 0, ZoneOffset.ofHours(2)))
                .build(),

            // Record with historical timestamptz values
            aValidTestRecord(3)
                .requiredTimestamptz(OffsetDateTime.of(1970, 1, 1, 0, 0, 1, 0, ZoneOffset.ofHours(2)))  // Unix epoch + 1 second
                .optionalTimestamptz(OffsetDateTime.of(2000, 1, 1, 0, 0, 30, 0, ZoneOffset.ofHours(2)))  // Y2K + 30 seconds
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
                    OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 0, ZoneOffset.ofHours(2))))
                .build(),

            // Record with various timestamptz ranges
            aValidTestRecord(7)
                .requiredListWithNullableElements(Arrays.asList(
                    null, OffsetDateTime.of(1970, 1, 1, 0, 0, 5, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2))))
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2023, 1, 1, 9, 15, 30, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 6, 15, 18, 45, 15, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2025, 12, 31, 23, 59, 59, 0, ZoneOffset.ofHours(2))))
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
                .optionalList(Arrays.asList(OffsetDateTime.of(2024, 3, 15, 10, 30, 10, 0, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2024, 9, 30, 16, 45, 30, 0, ZoneOffset.ofHours(2))))
                .optionalListWithNonNullElements(Arrays.asList(OffsetDateTime.of(2024, 4, 1, 8, 0, 10, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 8, 31, 17, 30, 45, 0, ZoneOffset.ofHours(2))))
                .build(),

            // Record with leap year timestamptz values (February 29th with timezone awareness)
            aValidTestRecord(11)
                .requiredTimestamptz(OffsetDateTime.of(2024, 2, 29, 12, 0, 10, 0, ZoneOffset.ofHours(2)))  // Leap year timestamptz
                .optionalTimestamptz(OffsetDateTime.of(2020, 2, 29, 23, 59, 59, 0, ZoneOffset.ofHours(2)))  // Another leap year timestamptz
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 29, 6, 30, 15, 10, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2020, 2, 29, 18, 45, 30, 15, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2000, 2, 29, 12, 0, 10, 50, ZoneOffset.ofHours(2))))
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 29, 9, 15, 45, 100, ZoneOffset.ofHours(2)), OffsetDateTime.of(2020, 2, 29, 15, 30, 10, 30, ZoneOffset.ofHours(2)), OffsetDateTime.of(2016, 2, 29, 21, 45, 15, 50, ZoneOffset.ofHours(2))))
                .optionalList(Arrays.asList(
                    null, OffsetDateTime.of(2024, 2, 29, 3, 15, 30, 50, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2012, 2, 29, 14, 30, 45, 40, ZoneOffset.ofHours(2)), null))
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2008, 2, 29, 11, 0, 10, 50, ZoneOffset.ofHours(2)), OffsetDateTime.of(2004, 2, 29, 20, 30, 15, 50, ZoneOffset.ofHours(2))))
                .build(),

            // Record with large lists (100 elements each for performance testing)
            aValidTestRecord(12)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 1, 12, 0, 10, 50, ZoneOffset.ofHours(2)))
                .optionalTimestamptz(OffsetDateTime.of(2024, 12, 31, 23, 59, 59, 30, ZoneOffset.ofHours(2)))
                .requiredListWithNullableElements(createLargeTimestamptzListWithNulls(100))
                .requiredListWithNonNullElements(createLargeTimestamptzListWithoutNulls(100))
                .optionalList(createOptionalLargeTimestamptzListWithNulls(100))  // Use version with nulls
                .optionalListWithNonNullElements(createOptionalLargeTimestamptzList(80))  // Use version without nulls
                .build(),

            // Record with microsecond precision (will be truncated to milliseconds by Kafka Connect)
            aValidTestRecord(13)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 123456000, ZoneOffset.ofHours(2)))  // 123.456 ms -> 123 ms (truncated)
                .optionalTimestamptz(OffsetDateTime.of(2024, 6, 30, 9, 15, 30, 987654000, ZoneOffset.ofHours(2)))   // 987.654 ms -> 987 ms (truncated)
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 3, 1, 10, 0, 10, 500000000, ZoneOffset.ofHours(2)),     // 500.000 ms = 500000 microseconds
                    null,
                    OffsetDateTime.of(2024, 8, 15, 16, 45, 12, 123456000, ZoneOffset.ofHours(2))))  // 123.456 ms = 123456 microseconds
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 5, 20, 8, 30, 45, 750000000, ZoneOffset.ofHours(2)),   // 750.000 ms = 750000 microseconds
                    OffsetDateTime.of(2024, 9, 10, 20, 15, 30, 999999000, ZoneOffset.ofHours(2)))) // 999.999 ms = 999999 microseconds
                .optionalList(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 14, 12, 0, 25, 111111000, ZoneOffset.ofHours(2)),    // 111.111 ms = 111111 microseconds
                    null,
                    OffsetDateTime.of(2024, 7, 4, 18, 30, 45, 666666000, ZoneOffset.ofHours(2))))  // 666.666 ms = 666666 microseconds
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 4, 10, 6, 45, 15, 333333000, ZoneOffset.ofHours(2)),   // 333.333 ms = 333333 microseconds
                    OffsetDateTime.of(2024, 10, 25, 22, 0, 10, 888888000, ZoneOffset.ofHours(2))))  // 888.888 ms = 888888 microseconds
                .microsecondTimestamptz(1705334445123456L) // 2024-01-15T14:30:45.123456Z
                .timestamptzStringArray(Arrays.asList(
                    "2024-03-01T10:00:00.500123+02",
                    "2024-08-15T16:45:12.123456+02",
                    "2024-05-20T08:30:45.750789+02"))
                .build(),

            // Record with nanosecond precision (should be truncated to milliseconds)
            // NOTE: Kafka Connect's Timestamp logical type only supports millisecond precision,
            // so any sub-millisecond data (microseconds/nanoseconds) will be truncated to milliseconds
            aValidTestRecord(14)
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 123456789, ZoneOffset.ofHours(2)))  // Should become 123000000 (123 milliseconds)
                .optionalTimestamptz(OffsetDateTime.of(2024, 6, 30, 9, 15, 30, 987654321, ZoneOffset.ofHours(2)))   // Should become 987000000 (987 milliseconds)
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 3, 1, 10, 0, 35, 500000123, ZoneOffset.ofHours(2)),     // Should become 500000000 (500 milliseconds)
                    null,
                    OffsetDateTime.of(2024, 8, 15, 16, 45, 12, 999999999, ZoneOffset.ofHours(2)))) // Should become 999000000 (999 milliseconds)
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 5, 20, 8, 30, 45, 750000456, ZoneOffset.ofHours(2)),   // Should become 750000000 (750 milliseconds)
                    OffsetDateTime.of(2024, 9, 10, 20, 15, 30, 111111111, ZoneOffset.ofHours(2)))) // Should become 111000000 (111 milliseconds)
                .optionalList(Arrays.asList(
                    OffsetDateTime.of(2024, 2, 14, 12, 0, 10, 222222222, ZoneOffset.ofHours(2)),    // Should become 222000000 (222 milliseconds)
                    null,
                    OffsetDateTime.of(2024, 7, 4, 18, 30, 45, 888888888, ZoneOffset.ofHours(2))))  // Should become 888000000 (888 milliseconds)
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 4, 10, 6, 45, 15, 444444444, ZoneOffset.ofHours(2)),   // Should become 444000000 (444 milliseconds)
                    OffsetDateTime.of(2024, 10, 25, 22, 0, 10, 777777777, ZoneOffset.ofHours(2))))  // Should become 777000000 (777 milliseconds)
                .microsecondTimestamptz(1719485730987654L) // 2024-06-27T10:55:30.987654Z
                .timestamptzStringArray(Arrays.asList(
                    "2024-03-01T10:00:00.500999+02",
                    "2024-08-15T16:45:12.999999+02",
                    "2024-02-14T12:00:00.222333+02",
                    "2024-07-04T18:30:45.888777+02"))
                .build()
        );
    }

    /**
     * Helper method to create a large list with nullable timestamptz elements.
     */
    private List<OffsetDateTime> createLargeTimestamptzListWithNulls(int size) {
        List<OffsetDateTime> result = new ArrayList<>();
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2024, 1, 1, 0, 0, 10, 0, ZoneOffset.ofHours(2));
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
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2023, 1, 1, 0, 0, 10, 0, ZoneOffset.ofHours(2));
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
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2025, 1, 1, 0, 0, 10, 0, ZoneOffset.ofHours(2));
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
        OffsetDateTime baseTimestamptz = OffsetDateTime.of(2025, 1, 1, 0, 0, 9, 0, ZoneOffset.ofHours(2));
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
                .requiredTimestamptz(OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2)))
                .optionalTimestamptz(OffsetDateTime.of(2024, 2, 28, 16, 45, 30, 0, ZoneOffset.ofHours(2)))
                .requiredListWithNullableElements(Arrays.asList(
                    OffsetDateTime.of(2024, 3, 1, 9, 0, 5, 0, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2024, 3, 31, 17, 30, 15, 0, ZoneOffset.ofHours(2)), null, OffsetDateTime.of(2024, 4, 15, 12, 15, 45, 0, ZoneOffset.ofHours(2))))
                .requiredListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 5, 1, 8, 30, 5, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 7, 31, 19, 15, 9, 0, ZoneOffset.ofHours(2))))
                .optionalList(Arrays.asList(
                    OffsetDateTime.of(2024, 8, 1, 7, 0, 5, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 9, 15, 14, 30, 45, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 10, 31, 20, 45, 15, 0, ZoneOffset.ofHours(2))))
                .optionalListWithNonNullElements(Arrays.asList(
                    OffsetDateTime.of(2024, 11, 1, 6, 15, 30, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 11, 15, 15, 0, 7, 0, ZoneOffset.ofHours(2)), OffsetDateTime.of(2024, 12, 1, 21, 30, 45, 0, ZoneOffset.ofHours(2))))
                .microsecondTimestamptz(1705334445123456L) // 2024-01-15T16:00:45.123456Z
                .timestamptzString("2024-01-15 14:30:45.123456+00")
                .timestamptzStringArray(Arrays.asList(
                    "2024-01-15 14:30:45.123456+02:00",
                    "2024-02-28 16:45:30.987654+02:00",
                    "2024-03-15 12:00:00.500000+02:00"));
    }

    /**
     * Creates the Firebolt table with TIMESTAMPTZ columns.
     */
    private Supplier<String> timestamptzTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredTimestamptz\" TIMESTAMPTZ NOT NULL, " +
                "\"optionalTimestamptz\" TIMESTAMPTZ NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TIMESTAMPTZ NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TIMESTAMPTZ NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TIMESTAMPTZ NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TIMESTAMPTZ NOT NULL) NULL, " +
                "\"microsecondTimestamptz\" TIMESTAMPTZ NULL, " +
                "\"timestamptzString\" TIMESTAMPTZ NULL, " +
                "\"timestamptzStringArray\" ARRAY(TIMESTAMPTZ NOT NULL) NULL" +
                ")";
    }

    /**
     * Publishes TimestamptzTestRecord messages to Kafka using JSON Schema serialization.
     * Converts LocalDateTime objects to longs (milliseconds since epoch) for Kafka Connect Timestamp logical type.
     */
    private void publishMessages(List<TimestamptzTestRecord> records) throws Exception {
        for (TimestamptzTestRecord record : records) {
            String key = "timestamptz-test-key-" + record.getRecordId();
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
            builder.timestamptzString(record.getTimestamptzString());

            expectedRecords.add(builder.build());
        }
        
        return expectedRecords;
    }
    
    /**
     * Truncates OffsetDateTime nanoseconds to microsecond precision.
     */
    private OffsetDateTime truncateToMicroseconds(OffsetDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        // Round nanoseconds to microseconds. If the 7th digit (hundreds of nanos) > 5, round up.
        int nanos = dateTime.getNano();
        int subMicroRemainder = nanos % 1_000; // last 3 digits (nanoseconds below a microsecond)
        int truncatedNanos = (nanos / 1_000) * 1_000; // drop last 3 digits

        // 7th digit check: hundreds place of the remainder (0-9)
        int hundredsOfNanos = subMicroRemainder / 100;
        if (hundredsOfNanos > 5) {
            int roundedNanos = truncatedNanos + 1_000; // add one microsecond
            if (roundedNanos == 1_000_000_000) {
                // carry into next second
                return dateTime.plusSeconds(1).withNano(0);
            }
            return dateTime.withNano(roundedNanos);
        }

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
            .collect(Collectors.toList());
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
            "\"optionalListWithNonNullElements\", \"microsecondTimestamptz\", \"timestamptzString\", \"timestamptzStringArray\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                TimestamptzTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                OffsetDateTime actualRequiredTimestamptz = rs.getTimestamp("requiredTimestamptz") != null ? 
                    rs.getTimestamp("requiredTimestamptz").toInstant().atOffset(ZoneOffset.ofHours(2)) : null;
                OffsetDateTime actualOptionalTimestamptz = rs.getTimestamp("optionalTimestamptz") != null ? 
                    rs.getTimestamp("optionalTimestamptz").toInstant().atOffset(ZoneOffset.ofHours(2)) : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                String timestamptzString = rs.getString("timestamptzString");
                // Retrieve new microsecond precision fields
                OffsetDateTime actualMicrosecondTimestamptz = parseString(rs.getString("microsecondTimestamptz"));

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
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex);
                    
                verifyTimestamptzArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex);
                
                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray, 
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyTimestamptzArray("optionalList", 
                        expected.getOptionalList(), actualOptionalListArray, recordIndex);
                }
                
                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray, 
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyTimestamptzArray("optionalListWithNonNullElements", 
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex);
                }
                
                // Verify microsecond precision fields (all records should have these fields)
                verifyMicrosecondTimestamptz(expected.getMicrosecondTimestamptz(), actualMicrosecondTimestamptz, recordIndex);
                List<OffsetDateTime> expectedTimestamptzArray = expected.getTimestamptzStringArray().stream().map(this::parseString)
                        .collect(Collectors.toList());
                verifyTimestamptzArray("timestamptzStringArray",
                        expectedTimestamptzArray, actualTimestamptzStringArray, recordIndex);
                assertEquals(expected.getTimestamptzString(), timestamptzString);
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }

    /**
     * Verifies a timestamptz array field using Array object instead of string parsing.
     */
    private void verifyTimestamptzArray(String fieldName, List<OffsetDateTime> expected, Array actualArray, 
                                      int recordIndex) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is TIMESTAMP (Types.TIMESTAMP = 93)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, baseType,
            fieldName + " should have base type TIMESTAMP (93) at index " + recordIndex);

        // Get the array as Timestamp array and convert to List<OffsetDateTime>
        java.sql.Timestamp[] arrayElements = (java.sql.Timestamp[]) actualArray.getArray();
        List<OffsetDateTime> actualList = Arrays.stream(arrayElements)
            .map(timestamp -> timestamp != null ? timestamp.toInstant().atOffset(ZoneOffset.ofHours(2)) : null)
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
     * Verifies microsecond precision timestamptz field.
     * Handles timezone variations (1-3 hour offsets) that may occur in test environments.
     */
    private void verifyMicrosecondTimestamptz(Long expectedMicroseconds, OffsetDateTime actualTimestamptz, int recordIndex) {
        assertNotNull(expectedMicroseconds, "Expected microsecondTimestamptz should not be null at index " + recordIndex);
        assertNotNull(actualTimestamptz, "Actual microsecondTimestamptz should not be null at index " + recordIndex);

        OffsetDateTime expected = fromMicros(expectedMicroseconds);
        assertEquals(expected.toInstant(), actualTimestamptz.toInstant());
    }

    // Convert expected microseconds to expected timestamp. Firebolt stores the data in UTC. We need to substract the default timezone of the test machine
    private OffsetDateTime fromMicros(long micros) {
        long seconds = micros / 1_000_000;
        int nanos = (int) (micros % 1_000_000) * 1000;  // Convert micros → nanos

        Instant instant = Instant.ofEpochSecond(seconds, nanos);
        return instant.atOffset(ZoneOffset.ofHours(0)); // assume UTC timezon
    }

    private OffsetDateTime parseString(String timeAsString) {
        return FireboltTimestamptzConverter.parseTimestamptz(timeAsString);
    }

} 