package com.firebolt.kafka.connect.integration.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.AllDataTypesTestRecord;
import com.firebolt.kafka.connect.integration.json.datatype.SimpleTestRecord;
import com.firebolt.kafka.connect.integration.json.datatype.TestStruct;
import com.firebolt.kafka.connect.utils.TestTag;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for JSON Schema serialization with Schema Registry and end-to-end Kafka Connect processing.
 * This test verifies:
 * 1. JSON schema registration with Schema Registry 
 * 2. JSON Schema serialization of SimpleTestRecord objects with schema references
 * 3. Publishing messages to Kafka topics using Schema Registry for schema management
 * 4. Kafka Connect connector registration with JSON Schema converter and topic-to-table mapping
 * 5. End-to-end message processing from Kafka to Firebolt with schema evolution support
 */
@Slf4j
@Tag(TestTag.NOT_IMPLEMENTED)
public class JsonSchemaSerializerTest extends BaseIntegrationTest {
    
    private static final String TABLE_NAME = "simple_test_table";
    private static final String TOPIC_NAME = "simple-test-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";
    private static final String DEFAULT_DATABASE_NAME = "integration_test_db";
    
    // All data types test constants
    private static final String ALL_DATA_TYPES_TABLE_NAME = "all_data_types_test_table";
    private static final String ALL_DATA_TYPES_TOPIC_NAME = "all-data-types-test-topic";
    private static final String ALL_DATA_TYPES_SCHEMA_SUBJECT = ALL_DATA_TYPES_TOPIC_NAME + "-value";
    
    private String allDataTypesConnectorName;
    private FireboltClient fireboltClient;
    
    // this is until we fix the offsetDateTime as it seems not to be working
    DateTimeFormatter OFFSET_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 1, 6, true)
            .appendPattern("XXX")  // Accepts +00 or +03:00
            .toFormatter();

    private Producer<String, SimpleTestRecord> producer;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        try {
            // Clean up existing schemas to avoid compatibility conflicts
            log.info("Cleaning up existing schemas to ensure clean state");
            try {
                getSchemaRegistryClient().deleteAllSchemas();
                // Wait briefly for cleanup to complete
                Thread.sleep(1000);
            } catch (Exception e) {
                log.warn("Failed to clean up schemas (may be empty): {}", e.getMessage());
            }
            
            // Generate unique connector names for this test run
            String testId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
            testConnectorName = "json-schema-test-connector-" + testId;
            allDataTypesConnectorName = "all-data-types-test-connector-" + testId;
            
            // Initialize Firebolt client
            fireboltClient = FireboltClient.createFor(DEFAULT_DATABASE_NAME);
            
            // Drop existing table if it exists (clean state)
            log.info("Ensuring clean state by dropping existing table: {}", TABLE_NAME);
            try {
                fireboltClient.dropTable(TABLE_NAME);
                log.info("Dropped existing table: {}", TABLE_NAME);
            } catch (Exception e) {
                log.debug("Table {} did not exist or couldn't be dropped: {}", TABLE_NAME, e.getMessage());
            }
            
            // Create the test table
            log.info("Creating Firebolt test table: {}", TABLE_NAME);
            fireboltClient.createSimpleTestTable(TABLE_NAME);
            
            // Create Kafka topic
            log.info("Creating Kafka topic: {}", TOPIC_NAME);
            createKafkaTopic(TOPIC_NAME);
            
            // Register JSON schema for SimpleTestRecord
            log.info("Registering JSON schema for SimpleTestRecord");
            registerJsonSchema();
            
            // Initialize object mapper and Kafka producer
            log.info("Initializing object mapper and Kafka producer");
            objectMapper = new ObjectMapper();
            initializeProducer();
            
            // Register the Kafka Connect connector
            log.info("Registering Kafka Connect connector: {}", testConnectorName);
            registerJsonConnector(testConnectorName, TOPIC_NAME, TOPIC_NAME + ":" + TABLE_NAME);
            
        } catch (Exception e) {
            log.error("Failed to set up test: {}", e.getMessage());
            throw new RuntimeException("Test setup failed", e);
        }
    }
    
    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }
        
        // Clean up connectors
        safelyDeleteConnector(testConnectorName);
        safelyDeleteConnector(allDataTypesConnectorName);
        
        // Clean up Firebolt tables
        if (fireboltClient != null) {
            try {
                fireboltClient.dropTable(TABLE_NAME);
                fireboltClient.dropTable(ALL_DATA_TYPES_TABLE_NAME);
                fireboltClient.close();
            } catch (SQLException e) {
                log.warn("Failed to drop tables or close client: {}", e.getMessage());
            }
        }
        
        // Clean up Kafka topics
        try {
            deleteKafkaTopic(TOPIC_NAME);
        } catch (Exception e) {
            log.warn("Failed to delete Kafka topic {}: {}", TOPIC_NAME, e.getMessage());
        }
        try {
            deleteKafkaTopic(ALL_DATA_TYPES_TOPIC_NAME);
        } catch (Exception e) {
            log.warn("Failed to delete Kafka topic {}: {}", ALL_DATA_TYPES_TOPIC_NAME, e.getMessage());
        }
        
        // Clean up schema registry
        try {
            getSchemaRegistryClient().deleteSchema(SCHEMA_SUBJECT);
        } catch (Exception e) {
            log.warn("Failed to delete schema {}: {}", SCHEMA_SUBJECT, e.getMessage());
        }
        try {
            getSchemaRegistryClient().deleteSchema(ALL_DATA_TYPES_SCHEMA_SUBJECT);
        } catch (Exception e) {
            log.warn("Failed to delete schema {}: {}", ALL_DATA_TYPES_SCHEMA_SUBJECT, e.getMessage());
        }
        
        super.tearDown();
    }

    @Test
    void testJsonSchemaSerializationAndKafkaConnectProcessing() {
        try {
            log.info("Starting end-to-end JSON Schema serialization and Kafka Connect processing test");
            
            // Generate 5 test messages
            List<SimpleTestRecord> testRecords = generateTestRecords(5);
            
            // Publish messages to Kafka using JSON serialization
            log.info("Publishing {} messages to Kafka topic: {}", testRecords.size(), TOPIC_NAME);
            publishMessages(testRecords);
            log.info("✅ Successfully published {} messages to Kafka with JSON serialization", testRecords.size());

            // Wait for connector to process messages (allow some time for processing)
            log.info("Waiting for Kafka Connect connector to process messages...");
            waitForDataInFirebolt(TABLE_NAME, testRecords.size());
            
            // Verify data was written to Firebolt table
            log.info("Verifying records were written to Firebolt table: {}", TABLE_NAME);
            verifyRecordsInFirebolt(testRecords);
        } catch (Exception e) {
            log.error("Test failed: {}", e.getMessage());
            throw new RuntimeException("Test execution failed", e);
        }
    }
    
    @Test
    @Disabled
    void testAllDataTypesJsonSchemaSerializationAndKafkaConnectProcessing() {
        try {
            log.info("Starting comprehensive all data types JSON Schema serialization and Kafka Connect processing test");
            
            // Setup for all data types test
            setupAllDataTypesTest();
            
            // Generate 5 test messages with different data patterns
            List<AllDataTypesTestRecord> testRecords = generateAllDataTypesTestRecords(5);
            
            // Publish messages to Kafka using JSON serialization
            log.info("Publishing {} all data types messages to Kafka topic: {}", testRecords.size(), ALL_DATA_TYPES_TOPIC_NAME);
            publishAllDataTypesMessages(testRecords);
            log.info("✅ Successfully published {} all data types messages to Kafka with JSON serialization", testRecords.size());

            // Wait for connector to process messages
            log.info("Waiting for Kafka Connect connector to process all data types messages...");
            waitForDataInFirebolt(ALL_DATA_TYPES_TABLE_NAME, testRecords.size());
            
            // Verify data was written to Firebolt table
            log.info("Verifying all data types records were written to Firebolt table: {}", ALL_DATA_TYPES_TABLE_NAME);
            verifyAllDataTypesRecordsInFirebolt(testRecords);
            
            log.info("✅ All data types test completed successfully");
        } catch (Exception e) {
            log.error("All data types test failed: {}", e.getMessage());
            throw new RuntimeException("All data types test execution failed", e);
        }
    }
    
    /**
     * Registers the JSON schema for SimpleTestRecord with the Schema Registry.
     * This schema is optimized for proper timestamp handling without oneOf ambiguity.
     */
    private void registerJsonSchema() throws Exception {
        // Schema that matches the SimpleTestRecord class structure
        String jsonSchema = "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Simple Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"id\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Unique identifier\"\n" +
                "    },\n" +
                "    \"createdAt\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int64\",\n" +
                "      \"connect.version\": 1,\n" +
                "      \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\",\n" +
                "      \"description\": \"Creation timestamp as Kafka Connect Timestamp logical type\"\n" +
                "    },\n" +
                "    \"recordTimestamp\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Epoch time in milliseconds for createdAt\"\n" +
                "    },\n" +
                "    \"title\": {\n" +
                "      \"type\": [\"string\", \"null\"],\n" +
                "      \"description\": \"Title text\"\n" +
                "    },\n" +
                "    \"description\": {\n" +
                "      \"type\": [\"string\", \"null\"],\n" +
                "      \"description\": \"Description text\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"id\", \"createdAt\", \"recordTimestamp\"]\n" +
                "}";
        
        log.info("Registering JSON schema for subject: {}", SCHEMA_SUBJECT);
        int schemaId = getSchemaRegistryClient().registerSchema(SCHEMA_SUBJECT, jsonSchema, "JSON");
        log.info("Successfully registered schema with ID: {}", schemaId);
    }
    
    /**
     * Initializes the Kafka producer with JSON Schema serializers for Schema Registry integration.
     */
    private void initializeProducer() {
        // Use the base class method to initialize the producer
        producer = initializeJsonProducer();
        log.info("Kafka producer initialized successfully using base class method");
    }
    
    /**
     * Generates test records for the test.
     */
    private List<SimpleTestRecord> generateTestRecords(int count) {
        List<SimpleTestRecord> records = new ArrayList<>();
        OffsetDateTime baseTime = OffsetDateTime.now();
        
        for (int i = 1; i <= count; i++) {
            OffsetDateTime createdAt = baseTime.plusMinutes(i);  // Generate timestamps offset by minutes
            SimpleTestRecord record = SimpleTestRecord.builder()
                .id((long) i)
                .createdAt(createdAt.toInstant().toEpochMilli())  // Convert to epoch millis for Kafka Connect Timestamp
                .recordTimestamp(createdAt.toInstant().toEpochMilli())  // Convert to epoch millis
                .title("Test Record " + i)
                .description("This is test record number " + i + " for JSON schema serialization test")
                .build();
            
            records.add(record);
        }
        
        log.info("Generated {} test records with timestamps", records.size());
        return records;
    }
    
    /**
     * Publishes test messages to Kafka using JSON Schema serialization with Schema Registry.
     */
    private void publishMessages(List<SimpleTestRecord> records) throws Exception {
        for (SimpleTestRecord record : records) {
            // Use the ID as the key (converted to string)
            String key = record.getId().toString();
            
            // Send the record object directly - JSON Schema serializer will handle serialization with schema reference
            ProducerRecord<String, SimpleTestRecord> producerRecord = 
                new ProducerRecord<>(TOPIC_NAME, key, record);
            
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message with key {}: {}", key, exception.getMessage());
                } else {
                    log.debug("Successfully sent message with key {} to partition {} at offset {}", 
                        key, metadata.partition(), metadata.offset());
                }
            }).get(); // Wait for completion
        }
        
        // Flush to ensure all messages are sent
        producer.flush();
        log.info("Successfully published {} messages to Kafka using JSON Schema serialization with Schema Registry", records.size());
    }
    
    /**
     * Waits for the expected number of records to be processed and written to Firebolt.
     */

    /**
     * Verifies that the published records exist in the Firebolt table.
     */
    private void verifyRecordsInFirebolt(List<SimpleTestRecord> expectedRecords) throws SQLException {
        // First, check the row count
        int actualCount = fireboltClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records in Firebolt table, but found " + actualCount);
        
        // Then verify the actual data
        String selectQuery = "SELECT id, \"createdAt\", \"recordTimestamp\", title, description FROM " + TABLE_NAME + " ORDER BY id";
        
        try (ResultSet rs = fireboltClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                SimpleTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                long actualId = rs.getLong("id");
//                OffsetDateTime actualCreatedAt = rs.getObject("createdAt", OffsetDateTime.class);
                // Handle null createdAt values
                String createdAt = rs.getString("createdAt");
                OffsetDateTime actualCreatedAt = null;
                if (createdAt != null) {
                    // Normalize offset: convert +03 → +03:00
                    createdAt = createdAt.replaceFirst("([+-]\\d{2})$", "$1:00");
                    actualCreatedAt = OffsetDateTime.parse(createdAt, OFFSET_DATE_FORMATTER);
                }
                Long actualRecordTimestamp = rs.getLong("recordTimestamp");
                String actualTitle = rs.getString("title");
                String actualDescription = rs.getString("description");
                
                assertEquals(expected.getId().longValue(), actualId, 
                    "ID mismatch for record " + recordIndex);
                assertEquals(expected.getRecordTimestamp(), actualRecordTimestamp, 
                    "RecordTimestamp mismatch for record " + recordIndex);
                assertEquals(expected.getTitle(), actualTitle, 
                    "Title mismatch for record " + recordIndex);
                assertEquals(expected.getDescription(), actualDescription, 
                    "Description mismatch for record " + recordIndex);
                
                // Compare timestamps using epoch milliseconds for more reliable comparison
                // This ensures we're comparing the actual moment in time, not the timezone representation
                if (expected.getCreatedAt() != null && actualCreatedAt != null) {
                    // Convert actual timestamp from database to epoch milliseconds for comparison
                    long actualEpochMillis = actualCreatedAt.toInstant().toEpochMilli();
                    assertEquals(expected.getCreatedAt(), actualEpochMillis,
                        "CreatedAt mismatch for record " + recordIndex + 
                        ". Expected epoch millis: " + expected.getCreatedAt() + 
                        ", Actual epoch millis: " + actualEpochMillis + 
                        " (Expected: " + expected.getCreatedAt() + ", Actual: " + actualCreatedAt + ")");
                } else {
                    assertEquals(expected.getCreatedAt(), actualCreatedAt,
                        "CreatedAt mismatch for record " + recordIndex + 
                        ". Expected: " + expected.getCreatedAt() + ", Actual: " + actualCreatedAt);
                }
                
                log.debug("Verified record {}: id={}, title={}", recordIndex, actualId, actualTitle);
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
        log.info("Successfully verified {} records in Firebolt table", expectedRecords.size());
    }
    

    
    /**
     * Gets the Kafka bootstrap servers configuration.
     */
    private String getKafkaBootstrapServers() {
        return System.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }
    
    /**
     * Gets the Schema Registry URL.
     */
    private String getSchemaRegistryUrl() {
        return System.getProperty("schema.registry.url", "http://localhost:8081");
    }

    /**
     * Deletes a Kafka topic using the AdminClient.
     */
    private void deleteKafkaTopic(String topicName) throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Deleting Kafka topic: {}", topicName);
        
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DeleteTopicsResult result = adminClient.deleteTopics(java.util.Collections.singletonList(topicName));
            
            // Wait for the topic deletion to complete
            result.all().get(60, TimeUnit.SECONDS);
            
            log.info("Successfully deleted Kafka topic: {}", topicName);
        }
    }
    
    /**
     * Sets up the environment for all data types test including table creation,
     * topic creation, schema registration, and connector registration.
     */
    private void setupAllDataTypesTest() throws Exception {
        log.info("Setting up all data types test environment");
        
        // Drop existing table if it exists (clean state)
        log.info("Ensuring clean state by dropping existing table: {}", ALL_DATA_TYPES_TABLE_NAME);
        try {
            fireboltClient.dropTable(ALL_DATA_TYPES_TABLE_NAME);
            log.info("Dropped existing table: {}", ALL_DATA_TYPES_TABLE_NAME);
        } catch (Exception e) {
            log.debug("Table {} did not exist or couldn't be dropped: {}", ALL_DATA_TYPES_TABLE_NAME, e.getMessage());
        }
        
        // Create the all data types test table
        log.info("Creating Firebolt all data types test table: {}", ALL_DATA_TYPES_TABLE_NAME);
        fireboltClient.createAllDataTypesTestTable(ALL_DATA_TYPES_TABLE_NAME);
        
        // Create Kafka topic for all data types
        log.info("Creating Kafka topic: {}", ALL_DATA_TYPES_TOPIC_NAME);
        createKafkaTopic(ALL_DATA_TYPES_TOPIC_NAME);
        
        // Manually register JSON schema for all data types
        log.info("Manually registering JSON schema for AllDataTypesTestRecord");
        registerAllDataTypesJsonSchema();
        
        // Register connector for all data types
        log.info("Registering Kafka Connect connector for all data types");
        registerJsonConnector(allDataTypesConnectorName, ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_TOPIC_NAME + ":" + ALL_DATA_TYPES_TABLE_NAME);
        
        log.info("All data types test environment setup completed");
    }
    
    /**
     * Generates test records for all data types testing.
     */
    private List<AllDataTypesTestRecord> generateAllDataTypesTestRecords(int count) {
        return Arrays.asList(
            // Complete record with typical values
            aValidAllDataTypesTestRecord(1)
                .build(),

            // Record with edge case values
            aValidAllDataTypesTestRecord(2)
                .colBigint(Long.MAX_VALUE)
                .colNumeric(new BigDecimal("99999999999999999999999999999.999999999"))
                .colReal(Float.MAX_VALUE)
                .colDoublePrecision(Double.MAX_VALUE)
                .colText("Edge Case Test Data with very long text that might exceed normal limits")
                .colBoolean(false)
                .colDate(LocalDate.of(9999, 12, 31))
                .colTimestamp(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999))
                .colTimestamptz(OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 999999999, ZoneOffset.UTC))
                .colBytea(Base64.getEncoder().encodeToString("edge_case_binary_data".getBytes()))
                .build(),

            // Record with nullable values
            aValidAllDataTypesTestRecord(3)
                .colBigint(null)
                .colNumeric(null)
                .colReal(null)
                .colDoublePrecision(null)
                .colText(null)
                .colBoolean(null)
                .colDate(null)
                .colTimestamp(null)
                .colTimestamptz(null)
                .colBytea(null)
                .colArrayTextNullable(null)
                .colArrayTextNotNull(null)
                .colArrayIntSyntax1(null)
                .colArrayIntSyntax2(null)
                .colArrayDate(null)
                .colArrayReal(null)
                .colArrayNested(null)
                .colArrayNumeric(null)
                .colArrayDoublePrecision(null)
                .colArrayTimestamptz(null)
                .colArrayTimestamp(null)
                .colStruct(null)
                .build(),

            // Record with geographic sample data
            aValidAllDataTypesTestRecord(4)
                .colText("San Francisco")
                .colArrayTextNullable(Arrays.asList("San Francisco", "New York", null, "London", "Tokyo"))
                .colArrayTextNotNull(Arrays.asList("California", "New York", "England", "Japan"))
                .colArrayIntSyntax1(Arrays.asList(37, 40, 51, 35))
                .colArrayIntSyntax2(Arrays.asList(774, 840, 130, 392))
                .colArrayDate(Arrays.asList(
                    LocalDate.of(2024, 1, 1),
                    LocalDate.of(2024, 1, 2),
                    LocalDate.of(2024, 1, 3)
                ))
                .colArrayReal(Arrays.asList(37.7749f, 40.7128f, 51.5074f, 35.6762f))
                .build(),

            // Record with variety of data patterns
            aValidAllDataTypesTestRecord(5)
                .colBigint(-1000L)
                .colNumeric(new BigDecimal("-12345678901234567890123456789.123456789"))
                .colReal(-1.5f)
                .colDoublePrecision(-1.23456789)
                .colText("Variety Test Data with special characters: !@#$%^&*()")
                .colBoolean(true)
                .colDate(LocalDate.of(1970, 1, 1))
                .colTimestamp(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0))
                .colTimestamptz(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
                .colBytea(Base64.getEncoder().encodeToString("variety_binary_data".getBytes()))
                .colArrayNested(Arrays.asList(
                    Arrays.asList(1, 2, 3),
                    Arrays.asList(4, 5),
                    Arrays.asList(6, 7, 8, 9)
                ))
                .colArrayNumeric(Arrays.asList(
                    new BigDecimal("100.123456789"),
                    new BigDecimal("200.987654321"), 
                    new BigDecimal("300.555555555")
                ))
                .colArrayDoublePrecision(Arrays.asList(1.11111, 2.22222, 3.33333, 4.44444))
                .colArrayTimestamptz(Arrays.asList(
                    OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
                    OffsetDateTime.of(2024, 1, 2, 13, 30, 0, 0, ZoneOffset.UTC),
                    OffsetDateTime.of(2024, 1, 3, 15, 45, 30, 0, ZoneOffset.UTC)
                ))
                .colArrayTimestamp(Arrays.asList(
                    LocalDateTime.of(2024, 1, 1, 12, 0, 0, 0),
                    LocalDateTime.of(2024, 1, 2, 13, 30, 0, 0),
                    LocalDateTime.of(2024, 1, 3, 15, 45, 30, 0)
                ))
                .colStruct(TestStruct.builder()
                    .name("variety")
                    .age(42)
                    .active(false)
                    .score(85.5)
                    .build())
                .build()
        );
    }

    /**
     * Helper method to create a valid AllDataTypesTestRecord with default values.
     */
    private AllDataTypesTestRecord.AllDataTypesTestRecordBuilder aValidAllDataTypesTestRecord(int colInteger) {
        return AllDataTypesTestRecord.builder()
            // Numeric types
            .colInteger(colInteger)
            .colBigint(1000L)
            .colNumeric(new BigDecimal("12345678901234567890123456789.123456789")) // Full NUMERIC(38,9) precision
            .colReal(1.5f)
            .colDoublePrecision(1.23456789)
            
            // Boolean type
            .colBoolean(true)
            
            // String type
            .colText("Basic Test Data")
            
            // Date and timestamp types
            .colDate(LocalDate.of(2024, 1, 1))
            .colTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 0, 0))
            .colTimestamptz(OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC))
            
            // Binary type - base64 encoded "hello"
            .colBytea(Base64.getEncoder().encodeToString("hello".getBytes()))
            
            // Array type with nullable elements
            .colArrayTextNullable(Arrays.asList("apple", null, "banana", "cherry"))
            
            // Array type with non-null elements only
            .colArrayTextNotNull(Arrays.asList("apple", "banana", "cherry", "date"))
            
            // Integer array types
            .colArrayIntSyntax1(Arrays.asList(1, 2, 3, 4, 5))
            .colArrayIntSyntax2(Arrays.asList(10, 20, 30, 40, 50))
            
            // Date and Real array types
            .colArrayDate(Arrays.asList(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 1, 2),
                LocalDate.of(2024, 1, 3)
            ))
            .colArrayReal(Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f))
            
            // Nested array type
            .colArrayNested(Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5),
                Arrays.asList(6, 7, 8, 9)
            ))
            
            // New array types
            .colArrayNumeric(Arrays.asList(
                new BigDecimal("100.123456789"),
                new BigDecimal("200.987654321"), 
                new BigDecimal("300.555555555")
            ))
            .colArrayDoublePrecision(Arrays.asList(1.11111, 2.22222, 3.33333, 4.44444))
            .colArrayTimestamptz(Arrays.asList(
                OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2024, 1, 2, 13, 30, 0, 0, ZoneOffset.UTC),
                OffsetDateTime.of(2024, 1, 3, 15, 45, 30, 0, ZoneOffset.UTC)
            ))
            .colArrayTimestamp(Arrays.asList(
                LocalDateTime.of(2024, 1, 1, 12, 0, 0, 0),
                LocalDateTime.of(2024, 1, 2, 13, 30, 0, 0),
                LocalDateTime.of(2024, 1, 3, 15, 45, 30, 0)
            ))
            
            // STRUCT type
            .colStruct(TestStruct.builder()
                .name("minimal")
                .age(25)
                .active(true)
                .score(100.0)
                .build());
    }
    
    /**
     * Publishes all data types messages to Kafka topic using JSON Schema serialization.
     */
    private void publishAllDataTypesMessages(List<AllDataTypesTestRecord> records) throws Exception {
        // Use the base class method to create producer for AllDataTypesTestRecord
        try (Producer<String, AllDataTypesTestRecord> allDataTypesProducer = initializeJsonProducer()) {
                    for (AllDataTypesTestRecord record : records) {
            log.info("DEBUG: Producing record with colNumeric: {} (type: {}, scale: {}, precision: {})", 
                    record.getColNumeric(), 
                    record.getColNumeric().getClass().getSimpleName(),
                    record.getColNumeric().scale(),
                    record.getColNumeric().precision());
            String key = "all-data-types-key-" + record.getColInteger();
            ProducerRecord<String, AllDataTypesTestRecord> producerRecord = 
                new ProducerRecord<>(ALL_DATA_TYPES_TOPIC_NAME, key, record);
            
            allDataTypesProducer.send(producerRecord).get(); // Wait for each message to be sent
            log.debug("Published all data types message with key: {}", key);
        }
            
            allDataTypesProducer.flush();
            log.info("Successfully published {} all data types messages", records.size());
        }
    }
    
    /**
     * Verifies that all data types records were properly written to Firebolt.
     */
    private void verifyAllDataTypesRecordsInFirebolt(List<AllDataTypesTestRecord> expectedRecords) throws SQLException {
        log.info("Verifying all data types records in Firebolt table: {}", ALL_DATA_TYPES_TABLE_NAME);
        
        // Count total records
        int actualCount = fireboltClient.countRows(ALL_DATA_TYPES_TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);

        
        // Verify specific records by checking the integer column (which is unique)
        String selectQuery = "SELECT \"colInteger\", \"colBigint\", \"colNumeric\", \"colReal\", \"colDoublePrecision\", \"colBoolean\", \"colText\", \"colDate\", \"colTimestamp\", \"colTimestamptz\", \"colBytea\", \"colArrayTextNullable\", \"colArrayTextNotNull\", \"colArrayIntSyntax1\", \"colArrayIntSyntax2\", \"colArrayDate\", \"colArrayReal\", \"colArrayNested\", \"colArrayNumeric\", \"colArrayDoublePrecision\", \"colArrayTimestamptz\", \"colArrayTimestamp\", \"colStruct\" FROM " + ALL_DATA_TYPES_TABLE_NAME + " ORDER BY \"colInteger\"";
        
        try (ResultSet rs = fireboltClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                AllDataTypesTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify key fields
                Integer actualColInteger = rs.getInt("colInteger");
                Long actualColBigint = rs.getLong("colBigint");
                BigDecimal actualColNumeric = rs.getBigDecimal("colNumeric");
                Float actualColReal = rs.getFloat("colReal");
                Double actualColDoublePrecision = rs.getDouble("colDoublePrecision");
                String actualColText = rs.getString("colText");
                Boolean actualColBoolean = rs.getBoolean("colBoolean");
                java.sql.Date actualColDate = rs.getDate("colDate");
                java.sql.Timestamp actualColTimestamp = rs.getTimestamp("colTimestamp");
                byte[] actualColBytea = rs.getBytes("colBytea");
                String actualColArrayTextNullable = rs.getString("colArrayTextNullable");
                String actualColArrayTextNotNull = rs.getString("colArrayTextNotNull");
                String actualColArrayIntSyntax1 = rs.getString("colArrayIntSyntax1");
                String actualColArrayIntSyntax2 = rs.getString("colArrayIntSyntax2");
                String actualColArrayDate = rs.getString("colArrayDate");
                String actualColArrayReal = rs.getString("colArrayReal");
                String actualColArrayNested = rs.getString("colArrayNested");
                String actualColArrayNumeric = rs.getString("colArrayNumeric");
                String actualColArrayDoublePrecision = rs.getString("colArrayDoublePrecision");
                String actualColArrayTimestamptz = rs.getString("colArrayTimestamptz");
                String actualColArrayTimestamp = rs.getString("colArrayTimestamp");
                String actualColStruct = rs.getString("colStruct");
                
                // For timestamptz, we need to handle the timestamp conversion
                java.sql.Timestamp actualColTimestamptz = rs.getTimestamp("colTimestamptz");
                
                assertEquals(expected.getColInteger(), actualColInteger, 
                    "ColInteger mismatch at index " + recordIndex);
                assertEquals(expected.getColBigint(), actualColBigint,
                    "ColBigint mismatch at index " + recordIndex);
                assertEquals(0, expected.getColNumeric().compareTo(actualColNumeric),
                    "ColNumeric mismatch at index " + recordIndex + " expected:" + expected.getColNumeric() +", but was: " + actualColNumeric);
                assertEquals(expected.getColReal(), actualColReal, 
                    "ColReal mismatch at index " + recordIndex);
                assertEquals(expected.getColDoublePrecision(), actualColDoublePrecision, 
                    "ColDoublePrecision mismatch at index " + recordIndex);
                assertEquals(expected.getColText(), actualColText, 
                    "ColText mismatch at index " + recordIndex);
                assertEquals(expected.getColBoolean(), actualColBoolean, 
                    "ColBoolean mismatch at index " + recordIndex);
                
                // Verify colDate field (convert java.sql.Date to LocalDate for comparison)
                if (actualColDate != null && expected.getColDate() != null) {
                    java.time.LocalDate actualLocalDate = actualColDate.toLocalDate();
                    assertEquals(expected.getColDate(), actualLocalDate,
                        "ColDate mismatch at index " + recordIndex);
                }
                
                // Verify colTimestamp field (convert java.sql.Timestamp to LocalDateTime for comparison)
                if (actualColTimestamp != null && expected.getColTimestamp() != null) {
                    java.time.LocalDateTime actualLocalDateTime = actualColTimestamp.toLocalDateTime();
                    assertEquals(expected.getColTimestamp(), actualLocalDateTime,
                        "ColTimestamp mismatch at index " + recordIndex);
                }
                
                // Verify colBytea field (decode base64 string before comparison)
                if (actualColBytea != null && expected.getColBytea() != null) {
                    byte[] expectedColBytea = Base64.getDecoder().decode(expected.getColBytea());
                    assertArrayEquals(expectedColBytea, actualColBytea,
                        "ColBytea mismatch at index " + recordIndex);
                }
                
                // Verify timestamptz field (convert to OffsetDateTime for comparison)
                if (actualColTimestamptz != null && expected.getColTimestamptz() != null) {
                    OffsetDateTime actualOffsetDateTime = actualColTimestamptz.toInstant().atOffset(ZoneOffset.UTC);
                    assertEquals(expected.getColTimestamptz(), actualOffsetDateTime,
                        "ColTimestamptz mismatch at index " + recordIndex);
                }
                
                // Verify colArrayTextNullable field (parse PostgreSQL array format)
                if (actualColArrayTextNullable != null && expected.getColArrayTextNullable() != null) {
                    try {
                        // Parse PostgreSQL array format: {apple,NULL,banana,cherry}
                        List<String> actualArray = parsePostgreSQLArray(actualColArrayTextNullable);
                        assertEquals(expected.getColArrayTextNullable(), actualArray,
                            "ColArrayTextNullable mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTextNullable PostgreSQL array: {}", actualColArrayTextNullable, e);
                        throw new RuntimeException("Failed to parse colArrayTextNullable PostgreSQL array", e);
                    }
                } else if (actualColArrayTextNullable == null && expected.getColArrayTextNullable() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayTextNullable and expected.getColArrayTextNullable() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayTextNullable(), actualColArrayTextNullable,
                        "ColArrayTextNullable null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayTextNotNull field (parse PostgreSQL array format)
                if (actualColArrayTextNotNull != null && expected.getColArrayTextNotNull() != null) {
                    try {
                        // Parse PostgreSQL array format: {apple,NULL,banana,cherry}
                        List<String> actualArray = parsePostgreSQLArray(actualColArrayTextNotNull);
                        assertEquals(expected.getColArrayTextNotNull(), actualArray,
                            "ColArrayTextNotNull mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTextNotNull PostgreSQL array: {}", actualColArrayTextNotNull, e);
                        throw new RuntimeException("Failed to parse colArrayTextNotNull PostgreSQL array", e);
                    }
                } else if (actualColArrayTextNotNull == null && expected.getColArrayTextNotNull() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayTextNotNull and expected.getColArrayTextNotNull() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayTextNotNull(), actualColArrayTextNotNull,
                        "ColArrayTextNotNull null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayIntSyntax1 field (parse PostgreSQL array format)
                if (actualColArrayIntSyntax1 != null && expected.getColArrayIntSyntax1() != null) {
                    try {
                        // Parse PostgreSQL array format: {1,2,3}
                        List<Integer> actualArray = parsePostgreSQLArray(actualColArrayIntSyntax1).stream()
                                .map(Integer::parseInt)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayIntSyntax1(), actualArray,
                            "ColArrayIntSyntax1 mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayIntSyntax1 PostgreSQL array: {}", actualColArrayIntSyntax1, e);
                        throw new RuntimeException("Failed to parse colArrayIntSyntax1 PostgreSQL array", e);
                    }
                } else if (actualColArrayIntSyntax1 == null && expected.getColArrayIntSyntax1() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayIntSyntax1 and expected.getColArrayIntSyntax1() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayIntSyntax1(), actualColArrayIntSyntax1,
                        "ColArrayIntSyntax1 null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayIntSyntax2 field (parse PostgreSQL array format)
                if (actualColArrayIntSyntax2 != null && expected.getColArrayIntSyntax2() != null) {
                    try {
                        // Parse PostgreSQL array format: {1,2,3}
                        List<Integer> actualArray = parsePostgreSQLArray(actualColArrayIntSyntax2).stream()
                                .map(Integer::parseInt)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayIntSyntax2(), actualArray,
                            "ColArrayIntSyntax2 mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayIntSyntax2 PostgreSQL array: {}", actualColArrayIntSyntax2, e);
                        throw new RuntimeException("Failed to parse colArrayIntSyntax2 PostgreSQL array", e);
                    }
                } else if (actualColArrayIntSyntax2 == null && expected.getColArrayIntSyntax2() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayIntSyntax2 and expected.getColArrayIntSyntax2() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayIntSyntax2(), actualColArrayIntSyntax2,
                        "ColArrayIntSyntax2 null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayDate field (parse PostgreSQL array format)
                if (actualColArrayDate != null && expected.getColArrayDate() != null) {
                    try {
                        // Parse PostgreSQL array format: {"2024-01-01","2024-01-02","2024-01-03"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayDate);
                        List<LocalDate> actualArray = actualStringArray.stream()
                                .map(LocalDate::parse)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayDate(), actualArray,
                            "ColArrayDate mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayDate PostgreSQL array: {}", actualColArrayDate, e);
                        throw new RuntimeException("Failed to parse colArrayDate PostgreSQL array", e);
                    }
                } else if (actualColArrayDate == null && expected.getColArrayDate() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayDate and expected.getColArrayDate() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayDate(), actualColArrayDate,
                        "ColArrayDate null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayReal field (parse PostgreSQL array format)
                if (actualColArrayReal != null && expected.getColArrayReal() != null) {
                    try {
                        // Parse PostgreSQL array format: {1.1,2.2,3.3}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayReal);
                        List<Float> actualArray = actualStringArray.stream()
                                .map(Float::parseFloat)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayReal(), actualArray,
                            "ColArrayReal mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayReal PostgreSQL array: {}", actualColArrayReal, e);
                        throw new RuntimeException("Failed to parse colArrayReal PostgreSQL array", e);
                    }
                } else if (actualColArrayReal == null && expected.getColArrayReal() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayReal and expected.getColArrayReal() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayReal(), actualColArrayReal,
                        "ColArrayReal null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayNested field (parse PostgreSQL array format)
                if (actualColArrayNested != null && expected.getColArrayNested() != null) {
                    try {
                        // Parse PostgreSQL nested array format: {{1,2},{3,4}}
                        List<List<Integer>> actualNestedArray = parsePostgreSQLNestedArray(actualColArrayNested);
                        assertEquals(expected.getColArrayNested(), actualNestedArray,
                            "ColArrayNested mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayNested PostgreSQL array: {}", actualColArrayNested, e);
                        throw new RuntimeException("Failed to parse colArrayNested PostgreSQL array", e);
                    }
                } else if (actualColArrayNested == null && expected.getColArrayNested() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayNested and expected.getColArrayNested() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayNested(), actualColArrayNested,
                        "ColArrayNested null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayNumeric field (parse PostgreSQL array format)
                if (actualColArrayNumeric != null && expected.getColArrayNumeric() != null) {
                    try {
                        // Parse PostgreSQL array format: {"100.123456789","200.987654321"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayNumeric);
                        List<BigDecimal> actualArray = actualStringArray.stream()
                                .map(BigDecimal::new)
                                .collect(Collectors.toList());
                        
                        // Compare BigDecimal arrays using compareTo() to ignore scale differences
                        assertEquals(expected.getColArrayNumeric().size(), actualArray.size(),
                            "ColArrayNumeric size mismatch at index " + recordIndex);
                        
                        for (int i = 0; i < expected.getColArrayNumeric().size(); i++) {
                            BigDecimal expectedValue = expected.getColArrayNumeric().get(i);
                            BigDecimal actualValue = actualArray.get(i);
                            assertEquals(0, expectedValue.compareTo(actualValue),
                                "ColArrayNumeric value mismatch at index " + recordIndex + ", element " + i + 
                                ": expected " + expectedValue + " but was " + actualValue);
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayNumeric PostgreSQL array: {}", actualColArrayNumeric, e);
                        throw new RuntimeException("Failed to parse colArrayNumeric PostgreSQL array", e);
                    }
                } else if (actualColArrayNumeric == null && expected.getColArrayNumeric() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayNumeric and expected.getColArrayNumeric() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayNumeric(), actualColArrayNumeric,
                        "ColArrayNumeric null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayDoublePrecision field (parse PostgreSQL array format)
                if (actualColArrayDoublePrecision != null && expected.getColArrayDoublePrecision() != null) {
                    try {
                        // Parse PostgreSQL array format: {1.11111,2.22222,3.33333}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayDoublePrecision);
                        List<Double> actualArray = actualStringArray.stream()
                                .map(Double::parseDouble)
                                .collect(Collectors.toList());
                        assertEquals(expected.getColArrayDoublePrecision(), actualArray,
                            "ColArrayDoublePrecision mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayDoublePrecision PostgreSQL array: {}", actualColArrayDoublePrecision, e);
                        throw new RuntimeException("Failed to parse colArrayDoublePrecision PostgreSQL array", e);
                    }
                } else if (actualColArrayDoublePrecision == null && expected.getColArrayDoublePrecision() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayDoublePrecision and expected.getColArrayDoublePrecision() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayDoublePrecision(), actualColArrayDoublePrecision,
                        "ColArrayDoublePrecision null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayTimestamptz field (parse PostgreSQL array format)
                if (actualColArrayTimestamptz != null && expected.getColArrayTimestamptz() != null) {
                    try {
                        // Parse PostgreSQL array format: {"2024-01-01T12:00:00Z","2024-01-02T13:30:00+02:00"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayTimestamptz);
                        List<OffsetDateTime> actualArray = actualStringArray.stream()
                                .map(this::normalizePostgreSQLTimestamp)
                                .map(OffsetDateTime::parse)
                                .collect(Collectors.toList());
                        
                        // Compare as Instant values since Firebolt normalizes all timestamps to UTC
                        List<java.time.Instant> expectedInstants = expected.getColArrayTimestamptz().stream()
                                .map(OffsetDateTime::toInstant)
                                .collect(Collectors.toList());
                        List<java.time.Instant> actualInstants = actualArray.stream()
                                .map(OffsetDateTime::toInstant)
                                .collect(Collectors.toList());
                                
                        assertEquals(expectedInstants, actualInstants,
                            "ColArrayTimestamptz mismatch at index " + recordIndex + " (comparing as Instant)");
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTimestamptz PostgreSQL array: {}", actualColArrayTimestamptz, e);
                        throw new RuntimeException("Failed to parse colArrayTimestamptz PostgreSQL array", e);
                    }
                } else if (actualColArrayTimestamptz == null && expected.getColArrayTimestamptz() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayTimestamptz and expected.getColArrayTimestamptz() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayTimestamptz(), actualColArrayTimestamptz,
                        "ColArrayTimestamptz null mismatch at index " + recordIndex);
                }
                
                // Verify colArrayTimestamp field (parse PostgreSQL array format)
                if (actualColArrayTimestamp != null && expected.getColArrayTimestamp() != null) {
                    try {
                        // Parse PostgreSQL array format: {"2024-01-01T12:00:00","2024-01-02T13:30:00"}
                        List<String> actualStringArray = parsePostgreSQLArray(actualColArrayTimestamp);
                        List<LocalDateTime> actualArray = actualStringArray.stream()
                                .map(s -> s.replace(" ", "T")) // Handle potential space formatting
                                .map(LocalDateTime::parse)
                                .collect(Collectors.toList());
                        
                        // Compare with microsecond precision since Firebolt rounds nanoseconds
                        List<LocalDateTime> expectedRounded = expected.getColArrayTimestamp().stream()
                                .map(this::roundToMicroseconds)
                                .collect(Collectors.toList());
                                
                        assertEquals(expectedRounded, actualArray,
                            "ColArrayTimestamp mismatch at index " + recordIndex + " (comparing with microsecond precision)");
                    } catch (Exception e) {
                        log.error("Failed to parse colArrayTimestamp PostgreSQL array: {}", actualColArrayTimestamp, e);
                        throw new RuntimeException("Failed to parse colArrayTimestamp PostgreSQL array", e);
                    }
                } else if (actualColArrayTimestamp == null && expected.getColArrayTimestamp() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColArrayTimestamp and expected.getColArrayTimestamp() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColArrayTimestamp(), actualColArrayTimestamp,
                        "ColArrayTimestamp null mismatch at index " + recordIndex);
                }
                
                // Verify colStruct field (parse JSON format)
                if (actualColStruct != null && expected.getColStruct() != null) {
                    try {
                        // Parse JSON struct format: {"name":"value","age":123,"active":true,"score":99.5}
                        TestStruct actualStruct = objectMapper.readValue(actualColStruct, TestStruct.class);
                        assertEquals(expected.getColStruct(), actualStruct,
                            "ColStruct mismatch at index " + recordIndex);
                    } catch (Exception e) {
                        log.error("Failed to parse colStruct JSON: {}", actualColStruct, e);
                        throw new RuntimeException("Failed to parse colStruct JSON", e);
                    }
                } else if (actualColStruct == null && expected.getColStruct() == null) {
                    // Both are null, which is valid
                    log.debug("Both actualColStruct and expected.getColStruct() are null");
                } else {
                    // One is null, the other is not - this is a mismatch
                    assertEquals(expected.getColStruct(), actualColStruct,
                        "ColStruct null mismatch at index " + recordIndex);
                }
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected " + expectedRecords.size() + " records but processed " + recordIndex);
        }
        
        log.info("✅ All data types records verification completed successfully");
    }
    
    /**
     * Registers JSON schema for AllDataTypesTestRecord.
     */
    private void registerAllDataTypesJsonSchema() throws Exception {
        // Schema that matches the AllDataTypesTestRecord class structure
        String jsonSchema = "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"All Data Types Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"colInteger\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Integer field (NOT NULL)\"\n" +
                "    },\n" +
                "    \"colBigint\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"integer\"}\n" +
                "      ],\n" +
                "      \"description\": \"Bigint field\"\n" +
                "    },\n" +
                "    \"colNumeric\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"string\",\n" +
                "          \"connect.type\": \"org.apache.kafka.connect.data.Decimal\",\n" +
                "          \"connect.parameters\": {\n" +
                "            \"scale\": \"9\"\n" +
                "          },\n" +
                "          \"description\": \"High-precision numeric value serialized as string to preserve full NUMERIC(38,9) precision\"\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    \"colReal\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\"}\n" +
                "      ],\n" +
                "      \"description\": \"Real field\"\n" +
                "    },\n" +
                "    \"colDoublePrecision\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\"}\n" +
                "      ],\n" +
                "      \"description\": \"Double precision field\"\n" +
                "    },\n" +
                "    \"colBoolean\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"boolean\"}\n" +
                "      ],\n" +
                "      \"description\": \"Boolean field\"\n" +
                "    },\n" +
                "    \"colText\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"Text field\"\n" +
                "    },\n" +
                "    \"colDate\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\", \"format\": \"date\"}\n" +
                "      ],\n" +
                "      \"description\": \"Date field\"\n" +
                "    },\n" +
                "    \"colTimestamp\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\", \"format\": \"date-time\"}\n" +
                "      ],\n" +
                "      \"description\": \"Timestamp field\"\n" +
                "    },\n" +
                "    \"colTimestamptz\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"integer\",\n" +
                "          \"connect.type\": \"int64\",\n" +
                "          \"connect.version\": 1,\n" +
                "          \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\",\n" +
                "          \"description\": \"Timestamptz field using Kafka Connect Timestamp logical type\"\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    \"colBytea\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"Bytea field\"\n" +
                "    },\n" +
                "    \"colArrayTextNullable\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"string\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Text array field with nullable elements\"\n" +
                "    },\n" +
                "    \"colArrayTextNotNull\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"string\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Text array field with non-null elements\"\n" +
                "    },\n" +
                "    \"colArrayIntSyntax1\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"integer\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Integer array field (syntax 1)\"\n" +
                "    },\n" +
                "    \"colArrayIntSyntax2\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"integer\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Integer array field (syntax 2)\"\n" +
                "    },\n" +
                "    \"colArrayDate\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"string\", \"format\": \"date\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Date array field\"\n" +
                "    },\n" +
                "    \"colArrayReal\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"number\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Real array field\"\n" +
                "    },\n" +
                "    \"colArrayNested\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"type\": \"array\",\n" +
                "            \"items\": {\"type\": \"integer\"}\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Nested array field\"\n" +
                "    },\n" +
                "    \"colArrayNumeric\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"string\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Numeric array field\"\n" +
                "    },\n" +
                "    \"colArrayDoublePrecision\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"number\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Double precision array field\"\n" +
                "    },\n" +
                "    \"colArrayTimestamptz\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"type\": \"integer\",\n" +
                "            \"connect.type\": \"int64\",\n" +
                "            \"connect.version\": 1,\n" +
                "            \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Timestamptz array field\"\n" +
                "    },\n" +
                "    \"colArrayTimestamp\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\"type\": \"string\", \"format\": \"date-time\"}\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Timestamp array field\"\n" +
                "    },\n" +
                "    \"colStruct\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\"}\n" +
                "      ],\n" +
                "      \"description\": \"Struct field stored as JSON text\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"colInteger\"]\n" +
                "}";

        int schemaId = getSchemaRegistryClient().registerSchema(ALL_DATA_TYPES_SCHEMA_SUBJECT, jsonSchema, "JSON");
        log.info("Schema registered successfully for subject '{}' with ID: {}", ALL_DATA_TYPES_SCHEMA_SUBJECT, schemaId);
    }

    /**
     * Parses a PostgreSQL array string into a List of strings.
     * Handles NULL values, quoted strings, and simple string arrays.
     * @param arrayString The PostgreSQL array string (e.g., {"San Francisco","New York",NULL,London,Tokyo})
     * @return A List of strings, with NULL values represented as null.
     */
    private List<String> parsePostgreSQLArray(String arrayString) {
        List<String> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return null; // Represent NULL as null
        }

        // Remove curly braces
        String content = arrayString.substring(1, arrayString.length() - 1);
        if (content.trim().isEmpty()) {
            return result; // Empty array
        }
        
        // Parse elements, handling quoted strings properly
        List<String> elements = parsePostgreSQLArrayElements(content);
        for (String element : elements) {
            String trimmedElement = element.trim();
            if (trimmedElement.equals("NULL")) {
                result.add(null); // PostgreSQL NULL becomes Java null
            } else if (trimmedElement.startsWith("\"") && trimmedElement.endsWith("\"")) {
                // Remove quotes from quoted strings
                result.add(trimmedElement.substring(1, trimmedElement.length() - 1));
            } else {
                result.add(trimmedElement);
            }
        }
        return result;
    }
    
    /**
     * Parses PostgreSQL array elements, properly handling quoted strings with commas.
     */
    private List<String> parsePostgreSQLArrayElements(String content) {
        List<String> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            
            if (c == '"' && (i == 0 || content.charAt(i-1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                elements.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        // Add the last element
        if (current.length() > 0) {
            elements.add(current.toString());
        }
        
        return elements;
    }

    /**
     * Parses a PostgreSQL nested array string into a List of Lists of Integers.
     * Handles NULL values, quoted strings, and simple nested arrays.
     * @param arrayString The PostgreSQL nested array string (e.g., {{1,2},{3,4}})
     * @return A List of Lists of Integers, with NULL values represented as null.
     */
    private List<List<Integer>> parsePostgreSQLNestedArray(String arrayString) {
        List<List<Integer>> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return null; // Represent NULL as null
        }

        // Remove outer curly braces
        String content = arrayString.substring(1, arrayString.length() - 1);
        if (content.trim().isEmpty()) {
            return result; // Empty array
        }
        
        // Use a more sophisticated approach to parse nested arrays
        // We need to track brace depth to properly identify inner arrays
        List<String> innerArrayStrings = parseNestedArrayElements(content);
        
        for (String innerArrayString : innerArrayStrings) {
            String trimmed = innerArrayString.trim();
            if (trimmed.equals("NULL")) {
                result.add(null); // PostgreSQL NULL becomes Java null
            } else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                // Parse the inner array
                List<Integer> innerArray = parsePostgreSQLArray(trimmed).stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                result.add(innerArray);
            } else {
                // This case should ideally not happen for a valid nested array
                log.warn("Unexpected element in nested array: {}", trimmed);
                result.add(null); // Or throw an error, depending on desired strictness
            }
        }
        return result;
    }
    
    /**
     * Parses nested array elements, properly handling brace-delimited inner arrays.
     * Example: {{1,2},{3,4}} -> ["{1,2}", "{3,4}"]
     */
    private List<String> parseNestedArrayElements(String content) {
        List<String> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int braceDepth = 0;
        
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            
            if (c == '{') {
                braceDepth++;
                current.append(c);
            } else if (c == '}') {
                braceDepth--;
                current.append(c);
                
                // If we've closed all braces, we've found a complete inner array
                if (braceDepth == 0) {
                    elements.add(current.toString());
                    current = new StringBuilder();
                }
            } else if (c == ',' && braceDepth == 0) {
                // Comma at depth 0 separates inner arrays
                if (current.length() > 0) {
                    elements.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        
        // Add the last element if there's any remaining content
        if (current.length() > 0) {
            elements.add(current.toString());
        }
        
        return elements;
    }
    
    /**
     * Normalizes PostgreSQL timestamp format to Java OffsetDateTime format.
     * PostgreSQL: "2024-01-01 12:00:00+00" -> Java: "2024-01-01T12:00:00+00:00"
     */
    private String normalizePostgreSQLTimestamp(String postgresTimestamp) {
        if (postgresTimestamp == null || postgresTimestamp.trim().isEmpty()) {
            return postgresTimestamp;
        }
        
        String normalized = postgresTimestamp.trim();
        
        // Replace space with 'T' between date and time
        normalized = normalized.replace(" ", "T");
        
        // Fix timezone format: convert +00 to +00:00, +02 to +02:00, etc.
        // Handle patterns like +00, -05, +09, etc.
        if (normalized.matches(".*[+-]\\d{2}$")) {
            normalized = normalized + ":00";
        }
        
        return normalized;
    }
    
    /**
     * Rounds a LocalDateTime to microsecond precision to match Firebolt's behavior.
     * Firebolt rounds nanoseconds to the nearest microsecond rather than truncating.
     */
    private LocalDateTime roundToMicroseconds(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        
        long nanos = dateTime.getNano();
        // Round to nearest microsecond (1000 nanoseconds = 1 microsecond)
        long microsInNanos = (nanos + 500) / 1000 * 1000;
        
        // Handle potential overflow to next second
        if (microsInNanos >= 1_000_000_000) {
            return dateTime.truncatedTo(java.time.temporal.ChronoUnit.SECONDS).plusSeconds(1);
        }
        
        return dateTime.withNano((int) microsInNanos);
    }
} 