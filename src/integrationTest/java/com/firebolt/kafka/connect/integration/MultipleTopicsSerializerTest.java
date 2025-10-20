package com.firebolt.kafka.connect.integration;

import com.firebolt.kafka.connect.integration.json.datatype.Record1TestRecord;
import com.firebolt.kafka.connect.integration.json.datatype.Record2TestRecord;
import com.firebolt.kafka.connect.integration.json.datatype.Record3TestRecord;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

import java.math.BigInteger;

@Slf4j
public class MultipleTopicsSerializerTest extends SchemaBaseIntegrationTest {
    
    // Topic and table names
    private static final String TOPIC1_NAME = generateTopicName("topic1");
    private static final String TOPIC2_NAME = generateTopicName("topic2");
    private static final String TOPIC3_NAME = generateTopicName("topic3");
    
    private static final String TABLE1_NAME = generateTableName("table1");
    private static final String TABLE2_NAME = generateTableName("table2");
    private static final String TABLE3_NAME = generateTableName("table3");
    
    private static final String SCHEMA1_SUBJECT = TOPIC1_NAME + "-value";
    private static final String SCHEMA2_SUBJECT = TOPIC2_NAME + "-value";
    private static final String SCHEMA3_SUBJECT = TOPIC3_NAME + "-value";

    private Producer<String, Record1TestRecord> producer1;
    private Producer<String, Record2TestRecord> producer2;
    private Producer<String, Record3TestRecord> producer3;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        
        // Generate unique connector name for this test run
        generateUniqueConnectorName("multiple-topics-serializer-test");
        
        // Setup individual resources for each topic (tables, topics, schemas)
        setupIndividualResources();
        
        // Setup the connector that handles all three topics
        setupMultiTopicConnector();
    }
    
    private void setupIndividualResources() {
        try {
            // Clean up any existing resources from previous test runs
            cleanupTestResources(TABLE1_NAME, TOPIC1_NAME, SCHEMA1_SUBJECT);
            cleanupTestResources(TABLE2_NAME, TOPIC2_NAME, SCHEMA2_SUBJECT);
            cleanupTestResources(TABLE3_NAME, TOPIC3_NAME, SCHEMA3_SUBJECT);
            
            // Create Firebolt tables
            log.info("Creating Firebolt test tables");
            String createTable1Sql = String.format(record1TableSchema().get(), TABLE1_NAME);
            fireboltDefaultDbClient.executeUpdate(createTable1Sql);
            
            String createTable2Sql = String.format(record2TableSchema().get(), TABLE2_NAME);
            fireboltDefaultDbClient.executeUpdate(createTable2Sql);
            
            String createTable3Sql = String.format(record3TableSchema().get(), TABLE3_NAME);
            fireboltDefaultDbClient.executeUpdate(createTable3Sql);
            
            // Create Kafka topics
            log.info("Creating Kafka topics");
            createKafkaTopic(TOPIC1_NAME);
            createKafkaTopic(TOPIC2_NAME);
            createKafkaTopic(TOPIC3_NAME);

            // Register JSON schemas
            log.info("Registering JSON schemas");
            String jsonSchema1 = record1JsonSchema().get();
            int schemaId1 = getSchemaRegistryClient().registerSchema(SCHEMA1_SUBJECT, jsonSchema1, "JSON");
            
            String jsonSchema2 = record2JsonSchema().get();
            int schemaId2 = getSchemaRegistryClient().registerSchema(SCHEMA2_SUBJECT, jsonSchema2, "JSON");
            
            String jsonSchema3 = record3JsonSchema().get();
            int schemaId3 = getSchemaRegistryClient().registerSchema(SCHEMA3_SUBJECT, jsonSchema3, "JSON");
            
            log.info("Successfully registered JSON schemas with IDs: {}, {}, {}", schemaId1, schemaId2, schemaId3);
            
        } catch (Exception e) {
            log.error("Failed to set up individual test resources: {}", e.getMessage());
            throw new RuntimeException("Individual test resources setup failed", e);
        }
    }
    
    private void setupMultiTopicConnector() {
        try {
            // Register the Kafka Connect connector for all three topics
            log.info("Registering Kafka Connect connector: {}", testConnectorName);
            
            // Topics: topic1,topic2,topic3
            String allTopics = TOPIC1_NAME + "," + TOPIC2_NAME + "," + TOPIC3_NAME;
            
            // Topic-to-table mappings: topic1:table1,topic2:topic2,topic3:topic3
            String topicToTableMappings = TOPIC1_NAME + ":" + TABLE1_NAME + "," + 
                                         TOPIC2_NAME + ":" + TABLE2_NAME + "," + 
                                         TOPIC3_NAME + ":" + TABLE3_NAME;
            
            registerJsonConnector(testConnectorName, allTopics, topicToTableMappings);
            
        } catch (Exception e) {
            log.error("Failed to set up multi-topic connector: {}", e.getMessage());
            throw new RuntimeException("Multi-topic connector setup failed", e);
        }
    }
    
    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer1 != null) {
            producer1.close();
        }
        if (producer2 != null) {
            producer2.close();
        }
        if (producer3 != null) {
            producer3.close();
        }
        
        // Clean up test resources for all topics
        cleanupTestResources(TABLE1_NAME, TOPIC1_NAME, SCHEMA1_SUBJECT);
        cleanupTestResources(TABLE2_NAME, TOPIC2_NAME, SCHEMA2_SUBJECT);
        cleanupTestResources(TABLE3_NAME, TOPIC3_NAME, SCHEMA3_SUBJECT);
        
        super.tearDown();
    }

    @ParameterizedTest
    @CsvSource({
        "true,  'WITH null fields included in JSON as field: null'",
        "false, 'WITH null fields omitted from JSON entirely'"
    })
    void testMultipleTopicsSerialization(boolean includeNulls, String testDescription) throws Exception {
        log.info("Starting test with includeNulls={}, description={}", includeNulls, testDescription);
        
        producer1 = initializeJsonProducer(includeNulls);
        producer2 = initializeJsonProducer(includeNulls);
        producer3 = initializeJsonProducer(includeNulls);
        
        // Create test records
        List<Record1TestRecord> record1List = createRecord1TestRecords();
        List<Record2TestRecord> record2List = createRecord2TestRecords();
        List<Record3TestRecord> record3List = createRecord3TestRecords();
        
        log.info("Created {} Record1 records, {} Record2 records, {} Record3 records", 
            record1List.size(), record2List.size(), record3List.size());
        
        // Publish messages intermingled (1 record to each topic in sequence)
        log.info("Publishing messages to topics: {}, {}, {}", TOPIC1_NAME, TOPIC2_NAME, TOPIC3_NAME);
        
        int maxRecords = Math.max(Math.max(record1List.size(), record2List.size()), record3List.size());
        for (int i = 0; i < maxRecords; i++) {
            // Publish Record1 to topic1
            if (i < record1List.size()) {
                ProducerRecord<String, Record1TestRecord> record1Message = 
                    new ProducerRecord<>(TOPIC1_NAME, "key-" + (i + 1), record1List.get(i));
                producer1.send(record1Message);
            }
            
            // Publish Record2 to topic2
            if (i < record2List.size()) {
                ProducerRecord<String, Record2TestRecord> record2Message = 
                    new ProducerRecord<>(TOPIC2_NAME, "key-" + (i + 1), record2List.get(i));
                producer2.send(record2Message);
            }
            
            // Publish Record3 to topic3
            if (i < record3List.size()) {
                ProducerRecord<String, Record3TestRecord> record3Message = 
                    new ProducerRecord<>(TOPIC3_NAME, "key-" + (i + 1), record3List.get(i));
                producer3.send(record3Message);
            }
        }
        
        // Flush all producers to ensure messages are sent
        producer1.flush();
        producer2.flush();
        producer3.flush();
        
        log.info("All messages published, waiting for data in Firebolt tables");
        
        // Wait for data in Firebolt tables
        log.info("Waiting for {} records in table {}", record1List.size(), TABLE1_NAME);
        waitForDataInFirebolt(TABLE1_NAME, record1List.size());
        
        log.info("Waiting for {} records in table {}", record2List.size(), TABLE2_NAME);
        waitForDataInFirebolt(TABLE2_NAME, record2List.size());
        
        log.info("Waiting for {} records in table {}", record3List.size(), TABLE3_NAME);
        waitForDataInFirebolt(TABLE3_NAME, record3List.size());
        
        log.info("All data received, verifying records");
        
        // Verify all records in their respective tables
        verifyRecord1InFirebolt(record1List);
        verifyRecord2InFirebolt(record2List);
        verifyRecord3InFirebolt(record3List);
        
        log.info("Test completed successfully");
    }

    /**
     * Creates 30 test records for Record1 (topic1 -> table1).
     */
    private List<Record1TestRecord> createRecord1TestRecords() {
        List<Record1TestRecord> records = new ArrayList<>();
        
        for (int i = 1; i <= 30; i++) {
            records.add(Record1TestRecord.builder()
                .id(i)
                .text("Record1-" + i)
                .build());
        }
        
        return records;
    }

    /**
     * Creates 30 test records for Record2 (topic2 -> table2).
     */
    private List<Record2TestRecord> createRecord2TestRecords() {
        List<Record2TestRecord> records = new ArrayList<>();
        
        for (int i = 1; i <= 30; i++) {
            records.add(Record2TestRecord.builder()
                .id(i)
                .value(10.5f + i * 0.1f)
                .bigIntAttribute(BigInteger.valueOf(1000000000L + i * 1000000L))
                .build());
        }
        
        return records;
    }

    /**
     * Creates 30 test records for Record3 (topic3 -> table3).
     */
    private List<Record3TestRecord> createRecord3TestRecords() {
        List<Record3TestRecord> records = new ArrayList<>();
        
        for (int i = 1; i <= 30; i++) {
            records.add(Record3TestRecord.builder()
                .id(i)
                .userIds(Arrays.asList(i * 10, i * 20, i * 30))
                .build());
        }
        
        return records;
    }
    
    private Supplier<String> record1TableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"text\" TEXT NOT NULL" +
                ")";
    }
    
    private Supplier<String> record2TableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"value\" FLOAT NOT NULL, " +
                "\"bigIntAttribute\" BIGINT NOT NULL" +
                ")";
    }
    
    private Supplier<String> record3TableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"userIds\" ARRAY(INTEGER) NOT NULL" +
                ")";
    }
    
    private Supplier<String> record1JsonSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Record1 Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"id\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record ID\"\n" +
                "    },\n" +
                "    \"text\": {\n" +
                "      \"type\": \"string\",\n" +
                "      \"description\": \"Text field\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"id\", \"text\"]\n" +
                "}";
    }
    
    private Supplier<String> record2JsonSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Record2 Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"id\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record ID\"\n" +
                "    },\n" +
                "    \"value\": {\n" +
                "      \"type\": \"number\",\n" +
                "      \"description\": \"Value field\"\n" +
                "    },\n" +
                "    \"bigIntAttribute\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"description\": \"Big integer attribute\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"id\", \"value\", \"bigIntAttribute\"]\n" +
                "}";
    }
    
    private Supplier<String> record3JsonSchema() {
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"Record3 Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"id\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
                "      \"description\": \"Record ID\"\n" +
                "    },\n" +
                "    \"userIds\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\"type\": \"integer\"},\n" +
                "      \"description\": \"List of user IDs\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"id\", \"userIds\"]\n" +
                "}";
    }
    
    /**
     * Verifies that Record1 records are correctly stored in Firebolt table1.
     */
    private void verifyRecord1InFirebolt(List<Record1TestRecord> expectedRecords) throws SQLException {
        log.debug("Verifying {} Record1 records in table {}", expectedRecords.size(), TABLE1_NAME);
        
        // Verify specific records by id
        String selectQuery = String.format(
            "SELECT \"id\", \"text\" FROM \"%s\" ORDER BY \"id\"", TABLE1_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            while (rs.next() && recordIndex < expectedRecords.size()) {
                Record1TestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualId = rs.getInt("id");
                String actualText = rs.getString("text");
                
                assertEquals(expected.getId(), actualId, 
                    "Record1 ID mismatch at index " + recordIndex);
                assertEquals(expected.getText(), actualText, 
                    "Record1 text mismatch at index " + recordIndex);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected " + expectedRecords.size() + " Record1 records, but found " + recordIndex);
        }
    }

    /**
     * Verifies that Record2 records are correctly stored in Firebolt table2.
     */
    private void verifyRecord2InFirebolt(List<Record2TestRecord> expectedRecords) throws SQLException {
        log.debug("Verifying {} Record2 records in table {}", expectedRecords.size(), TABLE2_NAME);
        
        // Verify specific records by id
        String selectQuery = String.format(
            "SELECT \"id\", \"value\", \"bigIntAttribute\" FROM \"%s\" ORDER BY \"id\"", TABLE2_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            while (rs.next() && recordIndex < expectedRecords.size()) {
                Record2TestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualId = rs.getInt("id");
                Float actualValue = rs.getFloat("value");
                BigInteger actualBigIntAttribute = rs.getBigDecimal("bigIntAttribute").toBigInteger();
                
                assertEquals(expected.getId(), actualId, 
                    "Record2 ID mismatch at index " + recordIndex);
                assertEquals(expected.getValue(), actualValue, 
                    "Record2 value mismatch at index " + recordIndex);
                assertEquals(expected.getBigIntAttribute(), actualBigIntAttribute, 
                    "Record2 bigIntAttribute mismatch at index " + recordIndex);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected " + expectedRecords.size() + " Record2 records, but found " + recordIndex);
        }
    }

    /**
     * Verifies that Record3 records are correctly stored in Firebolt table3.
     */
    private void verifyRecord3InFirebolt(List<Record3TestRecord> expectedRecords) throws SQLException {
        log.debug("Verifying {} Record3 records in table {}", expectedRecords.size(), TABLE3_NAME);
        
        // Verify specific records by id
        String selectQuery = String.format(
            "SELECT \"id\", \"userIds\" FROM \"%s\" ORDER BY \"id\"", TABLE3_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            while (rs.next() && recordIndex < expectedRecords.size()) {
                Record3TestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualId = rs.getInt("id");
                Array actualUserIdsArray = rs.getArray("userIds");
                
                assertEquals(expected.getId(), actualId, 
                    "Record3 ID mismatch at index " + recordIndex);
                
                // Convert array to list for comparison
                if (actualUserIdsArray != null) {
                    Integer[] actualUserIds = (Integer[]) actualUserIdsArray.getArray();
                    List<Integer> actualUserIdsList = Arrays.asList(actualUserIds);
                    assertEquals(expected.getUserIds(), actualUserIdsList, 
                        "Record3 userIds mismatch at index " + recordIndex);
                } else {
                    assertNull(expected.getUserIds(), 
                        "Record3 userIds should be null at index " + recordIndex);
                }
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected " + expectedRecords.size() + " Record3 records, but found " + recordIndex);
        }
    }
} 