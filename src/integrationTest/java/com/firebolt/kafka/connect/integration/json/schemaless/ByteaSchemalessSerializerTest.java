package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.ByteaTestRecord;
import com.firebolt.kafka.connect.utils.TestTag;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag(value = TestTag.NOT_IMPLEMENTED)
public class ByteaSchemalessSerializerTest extends SchemalessBaseIntegrationTest {
    
    private static final String TABLE_NAME = "bytea_test_table";
    private static final String TOPIC_NAME = "bytea-test-topic";

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("bytea-serializer-test-schemaless");
        
        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, byteaTableSchema());
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
    @CsvSource({
        "true,  'WITH null fields included in JSON as field: null'",
        "false, 'WITH null fields omitted from JSON entirely'"
    })
    void testByteaSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeSchemalessJsonProducer(includeNulls);
        
        List<ByteaTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyByteaRecordsInFirebolt(testRecords);
    }

    /**
     * Creates test records covering all binary data scenarios and edge cases.
     * Note: All binary data is UTF-8 safe to avoid encoding issues with Firebolt.
     */
    private List<ByteaTestRecord> createTestRecords() {
        return Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with minimal binary data (single byte)
            aValidTestRecord(2)
                .requiredBytea(new byte[]{0x01})
                .optionalBytea(new byte[]{0x02})
                .requiredListWithNullableElements(Arrays.asList(
                    "item1".getBytes(StandardCharsets.UTF_8),
                    "item2".getBytes(StandardCharsets.UTF_8)))
                .requiredListWithNonNullElements(Arrays.asList(
                    "value1".getBytes(StandardCharsets.UTF_8),
                    "value2".getBytes(StandardCharsets.UTF_8)))
                .optionalList(Arrays.asList(
                    "opt1".getBytes(StandardCharsets.UTF_8)))
                .optionalListWithNonNullElements(Arrays.asList(
                    "choice1".getBytes(StandardCharsets.UTF_8)))
                .build(),

            // Record with null optional bytea
            aValidTestRecord(3)
                .optionalBytea(null)
                .build(),

            // Record with single byte data
            aValidTestRecord(4)
                .requiredBytea(new byte[]{0x42})
                .optionalBytea(new byte[]{0x41})
                .build(),

            // Record with ASCII text as binary
            aValidTestRecord(5)
                .requiredBytea("Hello World".getBytes(StandardCharsets.UTF_8))
                .optionalBytea("Binary ASCII".getBytes(StandardCharsets.UTF_8))
                .build(),

            // Record with unicode text as binary
            aValidTestRecord(6)
                .requiredBytea("Hello 世界! 🌍".getBytes(StandardCharsets.UTF_8))
                .optionalBytea("🚀✨🎯".getBytes(StandardCharsets.UTF_8))
                .build(),

            // Record with UTF-8 safe edge case bytes
            aValidTestRecord(7)
                .requiredBytea("Edge case: \n\r\t".getBytes(StandardCharsets.UTF_8))
                .optionalBytea("Special chars: !@#$%^&*()".getBytes(StandardCharsets.UTF_8))
                .build(),

            // Record with large binary data
            aValidTestRecord(8)
                .requiredBytea(createLargeTextData(1000))
                .optionalBytea("Large data test".getBytes(StandardCharsets.UTF_8))
                .build(),

            // Record with UTF-8 safe binary data
            aValidTestRecord(9)
                .requiredBytea("Random-like UTF-8 safe data with special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?~`".getBytes(StandardCharsets.UTF_8))
                .optionalBytea("More UTF-8 safe content: 1234567890abcdef".getBytes(StandardCharsets.UTF_8))
                .build(),

            // Record with mixed array content
            aValidTestRecord(10)
                .optionalBytea(new byte[0])
                .requiredListWithNullableElements(Arrays.asList(
                    "Test".getBytes(StandardCharsets.UTF_8),
                    null,
                    "Value".getBytes(StandardCharsets.UTF_8),
                    new byte[0],
                    "Final".getBytes(StandardCharsets.UTF_8)
                ))
                .requiredListWithNonNullElements(Arrays.asList(
                    "Item1".getBytes(StandardCharsets.UTF_8),
                    "Item2".getBytes(StandardCharsets.UTF_8),
                    "Item3".getBytes(StandardCharsets.UTF_8)
                ))
                .optionalList(Arrays.asList(
                    "Opt1".getBytes(StandardCharsets.UTF_8),
                    null,
                    "Opt3".getBytes(StandardCharsets.UTF_8)
                ))
                .optionalListWithNonNullElements(Arrays.asList(
                    "Choice1".getBytes(StandardCharsets.UTF_8),
                    "Choice2".getBytes(StandardCharsets.UTF_8)
                ))
                .build()
        );
    }

    /**
     * Creates the Firebolt table with proper null/non-null constraints for bytea testing.
     */
    private Supplier<String> byteaTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredBytea\" BYTEA NOT NULL, " +
                "\"optionalBytea\" BYTEA NULL, " +
                "\"stringAsBytea\" BYTEA NULL, " +
                "\"stringListAsBytea\" ARRAY(BYTEA NULL) NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(BYTEA NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(BYTEA NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(BYTEA NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(BYTEA NOT NULL) NULL" +
                ")";
    }

    /**
     * Publishes ByteaTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<ByteaTestRecord> records) throws Exception {
        for (ByteaTestRecord record : records) {
            String key = "bytea-test-key-" + record.getRecordId();
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
     * Verifies that the published bytea records exist in the Firebolt table with correct null handling.
     */
    private void verifyByteaRecordsInFirebolt(List<ByteaTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);
        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredBytea\", \"optionalBytea\", \"stringAsBytea\", \"stringListAsBytea\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                ByteaTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                byte[] actualRequiredBytea = rs.getBytes("requiredBytea");
                byte[] actualOptionalBytea = rs.getBytes("optionalBytea");
                
                // Read new string-mapped BYTEA columns
                byte[] actualStringAsBytea = rs.getBytes("stringAsBytea");
                Array actualStringListAsBytea = rs.getArray("stringListAsBytea");

                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertArrayEquals(expected.getRequiredBytea(), actualRequiredBytea, 
                    "RequiredBytea mismatch at index " + recordIndex);
                
                // Null handling verification for optional bytea
                if (expected.getOptionalBytea() == null) {
                    assertNull(actualOptionalBytea, 
                        "OptionalBytea should be null at index " + recordIndex);
                } else {
                    assertArrayEquals(expected.getOptionalBytea(), actualOptionalBytea, 
                        "OptionalBytea mismatch at index " + recordIndex);
                }
                
                // Verify new string-mapped BYTEA columns
                if (expected.getStringAsBytea() == null) {
                    assertNull(actualStringAsBytea);
                } else {
                    assertArrayEquals(expected.getStringAsBytea().getBytes(StandardCharsets.UTF_8), actualStringAsBytea);
                }
                if (expected.getStringListAsBytea() == null) {
                    assertNull(actualStringListAsBytea);
                } else {
                    // Convert expected strings to bytes
                    byte[][] expectedBytes = expected.getStringListAsBytea().stream()
                            .map(s -> s == null ? null : s.getBytes(StandardCharsets.UTF_8))
                            .toArray(byte[][]::new);
                    // Compare array contents
                    byte[][] actualBytes = (byte[][]) actualStringListAsBytea.getArray();
                    assertEquals(expectedBytes.length, actualBytes.length);
                    for (int i = 0; i < expectedBytes.length; i++) {
                        if (expectedBytes[i] == null) {
                            assertNull(actualBytes[i]);
                        } else {
                            assertArrayEquals(expectedBytes[i], actualBytes[i]);
                        }
                    }
                }

                // Array verification using getArray()
                verifyByteaArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex);
                    
                verifyByteaArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex);
                
                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray, 
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyByteaArray("optionalList", 
                        expected.getOptionalList(), actualOptionalListArray, recordIndex);
                }
                
                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray, 
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyByteaArray("optionalListWithNonNullElements", 
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex);
                }
                
                log.debug("Verified bytea record {}: recordId={}, requiredBytea length={}", 
                    recordIndex, actualRecordId, actualRequiredBytea != null ? actualRequiredBytea.length : 0);
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
    }
    
    /**
     * Verifies a bytea array field using Array object instead of string parsing.
     */
    private void verifyByteaArray(String fieldName, List<byte[]> expected, Array actualArray, 
                                int recordIndex) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is BINARY (Types.BINARY = -2)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.BINARY, baseType,
            fieldName + " should have base type BINARY (-2) at index " + recordIndex);

        // Get the array as byte array and convert to List<byte[]>
        byte[][] arrayElements = (byte[][]) actualArray.getArray();
        List<byte[]> actualList = Arrays.asList(arrayElements);

        // Direct list comparison
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            byte[] expectedElement = expected.get(i);
            byte[] actualElement = actualList.get(i);
            
            if (expectedElement == null) {
                assertNull(actualElement, 
                    fieldName + " element " + i + " should be null at index " + recordIndex);
            } else {
                assertArrayEquals(expectedElement, actualElement,
                    fieldName + " element " + i + " mismatch at index " + recordIndex);
            }
        }
    }

    /**
     * Creates large text data of specified size (UTF-8 safe).
     */
    private byte[] createLargeTextData(int size) {
        StringBuilder sb = new StringBuilder();
        String baseText = "This is a test string with some content. ";
        int baseLength = baseText.length();
        
        // Repeat the base text to reach the desired size
        int repetitions = size / baseLength + 1;
        for (int i = 0; i < repetitions; i++) {
            sb.append(baseText);
        }
        
        // Truncate to exact size
        String result = sb.toString().substring(0, size);
        return result.getBytes(StandardCharsets.UTF_8);
    }

    private ByteaTestRecord.ByteaTestRecordBuilder aValidTestRecord(int recordId) {
        return ByteaTestRecord.builder()
                .recordId(recordId)
                .requiredBytea("Default binary content".getBytes(StandardCharsets.UTF_8))
                .optionalBytea("Default optional value".getBytes(StandardCharsets.UTF_8))
                .stringAsBytea("Hello as bytea")
                .stringListAsBytea(Arrays.asList("one", "two", "three"))
                .requiredListWithNullableElements(Arrays.asList(
                    "item1".getBytes(StandardCharsets.UTF_8), 
                    "item2".getBytes(StandardCharsets.UTF_8)))
                .requiredListWithNonNullElements(Arrays.asList(
                    "value1".getBytes(StandardCharsets.UTF_8), 
                    "value2".getBytes(StandardCharsets.UTF_8)))
                .optionalList(Arrays.asList(
                    "opt1".getBytes(StandardCharsets.UTF_8)))
                .optionalListWithNonNullElements(Arrays.asList(
                    "choice1".getBytes(StandardCharsets.UTF_8)));
    }

} 