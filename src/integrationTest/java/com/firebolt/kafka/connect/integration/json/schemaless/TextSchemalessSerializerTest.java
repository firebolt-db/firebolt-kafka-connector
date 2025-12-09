package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
 
import com.firebolt.kafka.connect.integration.json.datatype.TextTestRecord;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TextSchemalessSerializerTest extends SchemalessBaseIntegrationTest {
    
    private static final String TABLE_NAME = generateTableName("text_test_table");
    private static final String TOPIC_NAME = generateTopicName("text-test-topic");

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("text-serializer-test");

        // moved setup to test methods
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
    void testTextSerialization(boolean includeNulls, Map<String, String> connectorOverrides, String testDescription) throws Exception {
        log.info("Running {} for text data type (schemaless)", testDescription);

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, textTableSchema(), connectorOverrides);

        producer = initializeSchemalessJsonProducer(includeNulls);
        
        List<TextTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyTextRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all scenarios including unicode, large text, and edge cases.
     */
    private List<TextTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with empty strings
            aValidTestRecord(2)
                .requiredText("")
                .optionalText("")
                .build(),

            // Record with unicode characters
            aValidTestRecord(3)
                .requiredText("Hello 世界! 🌍 Ñiño français العربية русский 日本語")
                .optionalText("🚀✨🎯💯🔥⭐️🎉💎🌟⚡️")
                .build(),

            // Record with special characters and escape sequences
            aValidTestRecord(4)
                .requiredText("Special chars: \t\n\r\"'\\\b\f")
                .optionalText("Quotes: \"double\" and 'single'")
                .build(),

            // Record with null optional text
            aValidTestRecord(5)
                .optionalText(null)
                .build(),

            // Record with very long text
            aValidTestRecord(6)
                .requiredText(generateLargeText(10000))
                .optionalText(generateRepeatingText("Long text pattern ", 100))
                .build(),

            // Record with empty but valid optional list
            aValidTestRecord(7)
                .optionalList(new ArrayList<>())
                .build(),

            // Record with null optional list
            aValidTestRecord(8)
                .optionalList(null)
                .build(),

            // Record with valid optional list (includes nulls)
            aValidTestRecord(9)
                .optionalList(Arrays.asList("first", null, "third"))
                .build(),

            // Record with valid optional list with null values, but empty array
            aValidTestRecord(10)
                .optionalListWithNonNullElements(new ArrayList<>())
                .build(),

            // Record with valid optional list with null values, but null
            aValidTestRecord(11)
                .optionalListWithNonNullElements(null)
                .build(),

            // Record with valid optional list without null values
            aValidTestRecord(12)
                .optionalListWithNonNullElements(Arrays.asList("non-null1", "non-null2", "non-null3"))
                .build(),

            // Record with large lists (1000 elements each)
            aValidTestRecord(13)
                .requiredText("Large lists test")
                .optionalText("Testing with many elements")
                .requiredListWithNullableElements(createLargeTextListWithNulls(1000))
                .requiredListWithNonNullElements(createLargeTextListWithoutNulls(1000))
                .optionalList(createOptionalLargeTextList(1000))
                .optionalListWithNonNullElements(createOptionalLargeTextList(500))  // Different size for variety
                .build(),

            // Record with unicode in arrays
            aValidTestRecord(14)
                .requiredText("Unicode arrays test")
                .optionalText("Testing unicode in arrays")
                .requiredListWithNullableElements(Arrays.asList("Hello", null, "世界", "🌍", null, "日本語"))
                .requiredListWithNonNullElements(Arrays.asList("non-null", "unicode", "🚀", "✨"))
                .optionalList(Arrays.asList("optional", null, "unicode", "🎯"))
                .optionalListWithNonNullElements(Arrays.asList("non-null", "optional", "unicode", "💯"))
                .build(),

            // Record with special characters in arrays
            // BUG:
            //   tabs are not handled properly:  "tab\tthere" -> is coming back as "tabthere"
            //   new line: "newline\nhere" -> newlinehere
            //   return : "return\rhere"
            //   backspace: "bell\bhere"
            aValidTestRecord(15)
                .requiredText("Special chars in arrays")
                .optionalText("Testing special characters")
                .requiredListWithNullableElements(Arrays.asList("normal", null, "quotes\"here", "backslash\\here", null))
                .requiredListWithNonNullElements(Arrays.asList("add special chars here"))
                .optionalList(Arrays.asList("optional", null, "special", "chars"))
                .optionalListWithNonNullElements(Arrays.asList("non-null", "special", "chars", "here"))
                .build()
        );
    }
    
    private Supplier<String> textTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredText\" TEXT NOT NULL, " +
                "\"optionalText\" TEXT NULL, " +
                "\"requiredInt\" TEXT NOT NULL, " +
                "\"requiredFloat\" TEXT NOT NULL, " +
                "\"requiredDouble\" TEXT NOT NULL, " +
                "\"requiredBigInt\" TEXT NOT NULL, " +
                "\"requiredBoolean\" TEXT NOT NULL, " +
                "\"requiredBigDecimal\" TEXT NOT NULL, " +
                "\"requiredLocalDate\" TEXT NOT NULL, " +
                "\"requiredLocalDateTime\" TEXT NOT NULL, " +
                "\"requiredTimestamptz\" TEXT NOT NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(TEXT NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(TEXT NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(TEXT NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(TEXT NOT NULL) NULL" +
                ")";
    }
    
    /**
     * Publishes TextTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<TextTestRecord> records) throws Exception {
        for (TextTestRecord record : records) {
            String key = "text-test-key-" + record.getRecordId();
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
     * Verifies that the published text records exist in the Firebolt table with correct null handling.
     */
    private void verifyTextRecordsInFirebolt(List<TextTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredText\", \"optionalText\", " +
            "\"requiredInt\", \"requiredFloat\", \"requiredDouble\", \"requiredBigInt\", \"requiredBoolean\", \"requiredBigDecimal\", \"requiredLocalDate\", \"requiredLocalDateTime\", \"requiredTimestamptz\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                TextTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                String actualRequiredText = rs.getString("requiredText");
                String actualOptionalText = rs.getString("optionalText");
                
                // Scalars stored as TEXT; read via getString
                String actualRequiredInt = rs.getString("requiredInt");
                String actualRequiredFloat = rs.getString("requiredFloat");
                String actualRequiredDouble = rs.getString("requiredDouble");
                String actualRequiredBigInt = rs.getString("requiredBigInt");
                String actualRequiredBoolean = rs.getString("requiredBoolean");
                String actualRequiredBigDecimal = rs.getString("requiredBigDecimal");
                String actualRequiredLocalDate = rs.getString("requiredLocalDate");
                String actualRequiredLocalDateTime = rs.getString("requiredLocalDateTime");
                String actualRequiredTimestamptz = rs.getString("requiredTimestamptz");

                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredText(), actualRequiredText, 
                    "RequiredText mismatch at index " + recordIndex);
                
                // Null handling verification for optional text
                if (expected.getOptionalText() == null) {
                    assertNull(actualOptionalText, 
                        "OptionalText should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalText(), actualOptionalText, 
                        "OptionalText mismatch at index " + recordIndex);
                }

                // Verify scalar TEXT columns as strings
                assertEquals(String.valueOf(expected.getRequiredInt()), actualRequiredInt);
                assertEquals(String.valueOf(expected.getRequiredFloat()), actualRequiredFloat);
                assertEquals(String.valueOf(expected.getRequiredDouble()), actualRequiredDouble);
                assertEquals(String.valueOf(expected.getRequiredBigInt()), actualRequiredBigInt);
                assertEquals(String.valueOf(expected.getRequiredBoolean()), actualRequiredBoolean);
                assertEquals(expected.getRequiredBigDecimal().toPlainString(), actualRequiredBigDecimal);

                // LocalDate and LocalDateTime serialized; compare string forms
                assertEquals("2024-01-15", actualRequiredLocalDate);
                assertEquals("2024-01-15T14:30:45.123", actualRequiredLocalDateTime);
                assertEquals("2024-01-15T14:30:45.123456Z", actualRequiredTimestamptz);
                
                // Array verification using getArray()
                verifyTextArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyTextArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                verifyTextArray("optionalList", 
                    expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                
                // Optional list with non-null elements verification
                verifyTextArray("optionalListWithNonNullElements", 
                    expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
    }
    
    /**
     * Verifies a text array field using Array object instead of string parsing.
     */
    private void verifyTextArray(String fieldName, List<String> expected, Array actualArray, 
                                int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is VARCHAR (Types.VARCHAR = 12)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.VARCHAR, baseType,
            fieldName + " should have base type VARCHAR (12) at index " + recordIndex);

        // Get the array as String array and convert to List
        String[] arrayElements = (String[]) actualArray.getArray();
        List<String> actualList = Arrays.asList(arrayElements);

        // Direct list comparison
        assertEquals(expected, actualList,
            fieldName + " mismatch at index " + recordIndex);
    }
    
    /**
     * Helper method to create a large text list with nullable elements.
     */
    private List<String> createLargeTextListWithNulls(int size) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (i % 5 == 0) {
                result.add(null);  // Every 5th element is null
            } else {
                result.add("Text element " + i + " with some content");
            }
        }
        return result;
    }
    
    /**
     * Helper method to create a large text list without null elements.
     */
    private List<String> createLargeTextListWithoutNulls(int size) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add("Non-null text element " + i);
        }
        return result;
    }
    
    /**
     * Helper method to create an optional large text list with different pattern.
     */
    private List<String> createOptionalLargeTextList(int size) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add("Optional text element " + i + " with pattern");
        }
        return result;
    }
    
    /**
     * Generates large text content of specified size in bytes.
     */
    private String generateLargeText(int sizeInBytes) {
        StringBuilder sb = new StringBuilder();
        String baseText = "This is a test string with unicode characters: 世界 🌍 العربية русский. ";
        
        while (sb.length() < sizeInBytes) {
            sb.append(baseText);
            // Add some variety with line numbers
            sb.append("Line ").append(sb.length() / baseText.length()).append(". ");
        }
        
        // Trim to exact size
        if (sb.length() > sizeInBytes) {
            sb.setLength(sizeInBytes);
        }
        
        return sb.toString();
    }
    
    /**
     * Generates repeating text pattern.
     */
    private String generateRepeatingText(String pattern, int repetitions) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < repetitions; i++) {
            sb.append(pattern);
        }
        return sb.toString();
    }

    private TextTestRecord.TextTestRecordBuilder aValidTestRecord(int recordId) {
        return TextTestRecord.builder()
                .recordId(recordId)
                .requiredText("Default required text")
                .optionalText("Default optional text")
                .requiredInt(42)
                .requiredFloat(3.14f)
                .requiredDouble(2.718281828)
                .requiredBigInt(1234567890123L)
                .requiredBoolean(true)
                .requiredBigDecimal(new java.math.BigDecimal("12345.6789"))
                .requiredLocalDate(java.time.LocalDate.of(2024, 1, 15))
                .requiredLocalDateTime(java.time.LocalDateTime.of(2024, 1, 15, 14, 30, 45, 123000000))
                .requiredTimestamptz(java.time.OffsetDateTime.of(2024, 1, 15, 14, 30, 45, 123456000, java.time.ZoneOffset.UTC))
                .requiredListWithNullableElements(Arrays.asList("first", null, "third", null, "fifth"))
                .requiredListWithNonNullElements(Arrays.asList("non-null1", "non-null2", "non-null3", "non-null4", "non-null5"))
                .optionalList(Arrays.asList("optional1", "optional2", "optional3"))
                .optionalListWithNonNullElements(Arrays.asList("non-null-opt1", "non-null-opt2", "non-null-opt3"));
    }

} 