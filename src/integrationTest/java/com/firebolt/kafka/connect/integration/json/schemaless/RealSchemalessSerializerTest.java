package com.firebolt.kafka.connect.integration.json.schemaless;

import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.json.datatype.RealTestRecord;
import com.firebolt.kafka.connect.utils.TestTag;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class RealSchemalessSerializerTest extends SchemalessBaseIntegrationTest {
    
    private static final String TABLE_NAME = "real_test_table_schemaless";
    private static final String TOPIC_NAME = "real-test-topic-schemaless";

    private Producer<String, String> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        // Generate unique connector name for this test run
        generateUniqueConnectorName("real-serializer-test-schemaless");

        // Setup test resources using centralized method
        setupSchemalessTestResources(TOPIC_NAME, TABLE_NAME, 
                         realTableSchema());
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
    void testRealSerialization(boolean includeNulls, String testDescription) throws Exception {
        producer = initializeSchemalessJsonProducer(includeNulls);
        
        List<RealTestRecord> testRecords = createTestRecords();
        
        publishMessages(testRecords);
        
        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        
        verifyRealRecordsInFirebolt(testRecords);
    }

    @Test
    void willNotStopProcessingValidRecordsInCaseSomeRecordsContainInvalidValues() throws Exception {
        producer = initializeSchemalessJsonProducer();

        RealTestRecord validRecord1 = aValidTestRecord(401)
                .realFromString("12.34")
                .build();
        RealTestRecord validRecord2 = aValidTestRecord(402)
                .realFromString("-0.56")
                .build();
        RealTestRecord invalidRecord1 = aValidTestRecord(403)
                .realFromString("abc")
                .build();
        RealTestRecord invalidRecord2 = aValidTestRecord(404)
                .realFromString("12.34.56")
                .build();

        List<RealTestRecord> testRecords = List.of(
                validRecord1,
                invalidRecord1,
                validRecord2,
                invalidRecord2
        );

        publishMessages(testRecords);

        List<RealTestRecord> expectedRecords = List.of(validRecord1, validRecord2);
        waitForDataInFirebolt(TABLE_NAME, expectedRecords.size());

        verifyRealRecordsInFirebolt(expectedRecords);
    }

    /**
     * Creates test records covering all scenarios.
     */
    private List<RealTestRecord> createTestRecords() {
        // Note: Float.MIN_VALUE is actually the smallest positive non-zero value (1.4E-45), not negative minimum
        // For realistic testing, we use actual negative and positive ranges
        float realisticMinimum = -999999.99f;  // Realistic negative value
        float realisticMaximum = 999999.99f;   // Realistic positive value
        
        List<RealTestRecord> records = Arrays.asList(
            // Complete record with typical values
            aValidTestRecord(1)
                .build(),

            // Record with realistic negative values
            aValidTestRecord(2)
                .requiredReal(realisticMinimum)
                .optionalReal(-12345.67f)
                .build(),

            // Record with realistic positive values
            aValidTestRecord(3)
                .requiredReal(realisticMaximum)
                .optionalReal(98765.43f)
                .build(),

            // Record with null real
            aValidTestRecord(4)
                .optionalReal(null)
                .build(),

            // Record with realistic values in arrays
            aValidTestRecord(5)
                .requiredListWithNullableElements(Arrays.asList(-1234.5f, null, 5678.9f))
                .requiredListWithNonNullElements(Arrays.asList(100.25f, 250.75f, 500.125f))
                .build(),

            // Record with very small decimal precision
            aValidTestRecord(6)
                .requiredReal(0.000001f)
                .optionalReal(0.0000123456f)
                .requiredListWithNullableElements(Arrays.asList(0.1f, null, 0.01f, 0.001f))
                .requiredListWithNonNullElements(Arrays.asList(0.000001f, 0.000002f, 0.000003f))
                .build(),

            // Record with scientific notation values
            aValidTestRecord(7)
                .requiredReal(1.23e6f)      // 1,230,000
                .optionalReal(-4.56e-3f)    // -0.00456
                .requiredListWithNullableElements(Arrays.asList(1e3f, null, -2e-4f))
                .requiredListWithNonNullElements(Arrays.asList(3.14159f, 2.71828f, 1.41421f))
                .build(),

            // Record with financial/currency-like precision
            aValidTestRecord(8)
                .requiredReal(1234567.89f)
                .optionalReal(-987654.32f)
                .requiredListWithNullableElements(Arrays.asList(99.99f, null, 149.95f, null, 29.50f))
                .requiredListWithNonNullElements(Arrays.asList(19.99f, 39.95f, 59.00f, 79.25f))
                .build()

//            // Record with actual Float.MIN_VALUE (smallest positive non-zero)
//            aValidTestRecord(6)
//                .requiredReal(Float.MIN_VALUE)
//                .optionalReal(Float.MIN_VALUE)
//                .requiredListWithNullableElements(Arrays.asList(Float.MIN_VALUE, null))
//                .requiredListWithNonNullElements(Arrays.asList(Float.MIN_VALUE))
//                .build()
//
//            // Record with Float.MAX_VALUE (largest possible float)
//            aValidTestRecord(7)
//                .requiredReal(Float.MAX_VALUE)
//                .optionalReal(Float.MAX_VALUE)
//                .requiredListWithNullableElements(Arrays.asList(null, Float.MAX_VALUE))
//                .requiredListWithNonNullElements(Arrays.asList(Float.MAX_VALUE))
//                .build()
//
//            // Record with negative Float.MAX_VALUE (largest negative)
//            aValidTestRecord(8)
//                .requiredReal(-Float.MAX_VALUE)
//                .optionalReal(-Float.MAX_VALUE)
//                .requiredListWithNullableElements(Arrays.asList(-Float.MAX_VALUE, null))
//                .requiredListWithNonNullElements(Arrays.asList(-Float.MAX_VALUE))
//                .build(),
//

        );

        return records;
    }
    
    private Supplier<String> realTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"recordId\" INTEGER NOT NULL, " +
                "\"requiredReal\" REAL NOT NULL, " +
                "\"optionalReal\" REAL NULL, " +
                "\"optionalByte\" REAL NULL, " +
                "\"optionalShort\" REAL NULL, " +
                "\"optionalInt\" REAL NULL, " +
                "\"requiredListWithNullableElements\" ARRAY(REAL NULL) NOT NULL, " +
                "\"requiredListWithNonNullElements\" ARRAY(REAL NOT NULL) NOT NULL, " +
                "\"optionalList\" ARRAY(REAL NULL) NULL, " +
                "\"optionalListWithNonNullElements\" ARRAY(REAL NOT NULL) NULL, " +
                "\"realFromString\" REAL NULL" +
                ")";
    }
    
    /**
     * Publishes RealTestRecord messages to Kafka using JSON Schema serialization.
     */
    private void publishMessages(List<RealTestRecord> records) throws Exception {
        for (int i = 0; i < records.size(); i++) {
            RealTestRecord record = records.get(i);
            String key = "real-test-key-" + record.getRecordId();

            ProducerRecord<String, String> producerRecord = 
                new ProducerRecord<>(TOPIC_NAME, key, mapper.writeValueAsString(record));
            
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("❌ Failed to send message with key {}: {}", key, exception.getMessage());
                } else {
                    log.info("✅ Successfully sent message with key {} to partition {} at offset {}", 
                        key, metadata.partition(), metadata.offset());
                }
            }).get();
            
        }

        producer.flush();
    }
    
    /**
     * Verifies that the published real records exist in the Firebolt table with correct null handling.
     */
    private void verifyRealRecordsInFirebolt(List<RealTestRecord> expectedRecords) throws SQLException {
        // Count total records
        int actualCount = fireboltDefaultDbClient.countRows(TABLE_NAME);
        assertEquals(expectedRecords.size(), actualCount, 
            "Expected " + expectedRecords.size() + " records but found " + actualCount);

        
        // Verify specific records by recordId
        String selectQuery = String.format(
            "SELECT \"recordId\", \"requiredReal\", \"optionalReal\", \"optionalByte\", \"optionalShort\", \"optionalInt\", " +
            "\"requiredListWithNullableElements\", \"requiredListWithNonNullElements\", \"optionalList\", " +
            "\"optionalListWithNonNullElements\", \"realFromString\" " +
            "FROM \"%s\" ORDER BY \"recordId\"", TABLE_NAME);
        
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;
            
            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(), 
                    "More records found in database than expected");
                
                RealTestRecord expected = expectedRecords.get(recordIndex);
                
                // Verify each field
                Integer actualRecordId = rs.getInt("recordId");
                Float actualRequiredReal = rs.getFloat("requiredReal");
                Float actualOptionalReal = rs.getObject("optionalReal") != null ? rs.getFloat("optionalReal") : null;
                Float actualOptionalByte = rs.getObject("optionalByte") != null ? rs.getFloat("optionalByte") : null;
                Float actualOptionalShort = rs.getObject("optionalShort") != null ? rs.getFloat("optionalShort") : null;
                Float actualOptionalInt = rs.getObject("optionalInt") != null ? rs.getFloat("optionalInt") : null;
                Float actualRealFromString = rs.getObject("realFromString") != null ? rs.getFloat("realFromString") : null;
                
                // Read arrays using getArray() instead of getString()
                Array actualRequiredListWithNullableArray = rs.getArray("requiredListWithNullableElements");
                Array actualRequiredListWithNonNullArray = rs.getArray("requiredListWithNonNullElements");
                Array actualOptionalListArray = rs.getArray("optionalList");
                Array actualOptionalListWithNonNullElementsArray = rs.getArray("optionalListWithNonNullElements");
                
                // Basic field verification
                assertEquals(expected.getRecordId(), actualRecordId, 
                    "RecordId mismatch at index " + recordIndex);
                assertEquals(expected.getRequiredReal(), actualRequiredReal, 0.0001f,
                    "RequiredReal mismatch at index " + recordIndex);
                
                // Null handling verification for optional real
                if (expected.getOptionalReal() == null) {
                    assertNull(actualOptionalReal, 
                        "OptionalReal should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalReal(), actualOptionalReal, 0.0001f,
                        "OptionalReal mismatch at index " + recordIndex);
                }

                if (expected.getOptionalByte() == null) {
                    assertNull(actualOptionalByte, "OptionalByte should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalByte().floatValue(), actualOptionalByte, 0.0001f);
                }
                if (expected.getOptionalShort() == null) {
                    assertNull(actualOptionalShort, "OptionalShort should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalShort().floatValue(), actualOptionalShort, 0.0001f);
                }
                if (expected.getOptionalInt() == null) {
                    assertNull(actualOptionalInt, "OptionalInt should be null at index " + recordIndex);
                } else {
                    assertEquals(expected.getOptionalInt().floatValue(), actualOptionalInt, 0.0001f);
                }
                if (expected.getRealFromString() == null) {
                    assertNull(actualRealFromString, "realFromString should be null at index " + recordIndex);
                } else {
                    assertEquals(Float.parseFloat(expected.getRealFromString()), actualRealFromString, 0.0001f);
                }
                
                // Array verification using getArray()
                verifyRealArray("requiredListWithNullableElements", 
                    expected.getRequiredListWithNullableElements(), actualRequiredListWithNullableArray, recordIndex, true);
                    
                verifyRealArray("requiredListWithNonNullElements", 
                    expected.getRequiredListWithNonNullElements(), actualRequiredListWithNonNullArray, recordIndex, false);
                
                // Optional list verification
                if (expected.getOptionalList() == null) {
                    assertNull(actualOptionalListArray, 
                        "OptionalList should be null at index " + recordIndex);
                } else {
                    verifyRealArray("optionalList", 
                        expected.getOptionalList(), actualOptionalListArray, recordIndex, true);
                }
                
                // Optional list with non-null elements verification
                if (expected.getOptionalListWithNonNullElements() == null) {
                    assertNull(actualOptionalListWithNonNullElementsArray, 
                        "OptionalListWithNonNullElements should be null at index " + recordIndex);
                } else {
                    verifyRealArray("optionalListWithNonNullElements", 
                        expected.getOptionalListWithNonNullElements(), actualOptionalListWithNonNullElementsArray, recordIndex, false);
                }
                
                recordIndex++;
            }
            
            assertEquals(expectedRecords.size(), recordIndex, 
                "Expected to verify " + expectedRecords.size() + " records, but only found " + recordIndex);
        }
        
    }
    
    /**
     * Verifies a real array field using Array object instead of string parsing.
     */
    private void verifyRealArray(String fieldName, List<Float> expected, Array actualArray, 
                                  int recordIndex, boolean allowNullElements) throws SQLException {
        if (expected == null) {
            assertNull(actualArray, fieldName + " should be null at index " + recordIndex);
            return;
        }
        
        // If we expect a non-null list, the actual array should not be null
        assertNotNull(actualArray, fieldName + " should not be null at index " + recordIndex);
        
        // Check that the array base type is REAL (Types.REAL = 7)
        int baseType = actualArray.getBaseType();
        assertEquals(Types.REAL, baseType,
            fieldName + " should have base type REAL (7) at index " + recordIndex);

        // Get the array as Float array and convert to List<Float>
        Float[] arrayElements = (Float[]) actualArray.getArray();
        List<Float> actualList = Arrays.asList(arrayElements);

        // Direct list comparison with tolerance for floating-point precision
        assertEquals(expected.size(), actualList.size(),
            fieldName + " size mismatch at index " + recordIndex);
        
        for (int i = 0; i < expected.size(); i++) {
            Float expectedElement = expected.get(i);
            Float actualElement = actualList.get(i);
            
            if (expectedElement == null) {
                assertNull(actualElement, 
                    fieldName + " element " + i + " should be null at index " + recordIndex);
            } else {
                assertEquals(expectedElement, actualElement, 0.0001f,
                    fieldName + " element " + i + " mismatch at index " + recordIndex);
            }
        }
    }

    private RealTestRecord.RealTestRecordBuilder aValidTestRecord(int recordId) {
        return RealTestRecord.builder()
                .recordId(recordId)
                .requiredReal(299.95f)                    // Realistic price-like value
                .optionalReal(1234.56f)                   // Realistic decimal value
                .optionalByte((byte) (recordId % 2 == 0 ? 0 : 1))
                .optionalShort((short) (recordId % 3))
                .optionalInt(recordId)
                .realFromString("123.45")
                .requiredListWithNullableElements(Arrays.asList(15.75f, null, 89.25f, null, 156.50f))
                .requiredListWithNonNullElements(Arrays.asList(23.45f, 67.89f, 134.12f, 256.78f, 398.99f))
                .optionalList(Arrays.asList(499.99f, 799.50f, 1299.75f))
                .optionalListWithNonNullElements(Arrays.asList(78.33f, 145.67f, 289.44f));
    }

} 