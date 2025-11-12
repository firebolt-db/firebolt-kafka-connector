package com.firebolt.kafka.connect.integration.customer;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.integration.SchemaBaseIntegrationTest;
import com.firebolt.kafka.connect.integration.SchemalessBaseIntegrationTest;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class Customer2IntegrationTest extends SchemaBaseIntegrationTest {

    // All data types test constants
    private static final String ALL_DATA_TYPES_TABLE_NAME = generateTableName("prepared-statement-test-table");
    private static final String ALL_DATA_TYPES_TOPIC_NAME = generateTopicName("prepared-statement-topic");

    private static final String ALL_DATA_TYPES_SCHEMA_SUBJECT = ALL_DATA_TYPES_TOPIC_NAME + "-value";

    private Producer<String, TestRecord> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);

        generateUniqueConnectorName("prepared-statement-connector");

        Map<String, String> connectorOverrideProperties = new HashMap<>();
        connectorOverrideProperties.put("optimize.inserts", "true");

        // Setup test resources using centralized method
        setupTestResources(ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_TABLE_NAME, ALL_DATA_TYPES_SCHEMA_SUBJECT, allDataTypesTableSchema(), allDataTypesJsonSchema(), connectorOverrideProperties);
    }

    @AfterEach
    protected void tearDown() {
        // Close producer
        if (producer != null) {
            producer.close();
        }

        // Clean up test resources
        cleanupTestResources(ALL_DATA_TYPES_TABLE_NAME, ALL_DATA_TYPES_TOPIC_NAME, ALL_DATA_TYPES_SCHEMA_SUBJECT);

        super.tearDown();
    }

    @ParameterizedTest
    @CsvSource({
            "false, 'WITH null fields omitted from JSON entirely'"
    })
    void testAllDataTypesJsonSchemaSerializationAndKafkaConnectProcessing(boolean includeNulls, String testDescription) throws Exception {
        producer =  initializeJsonProducer(includeNulls);

        // Generate 5 test messages with different data patterns
        List<TestRecord> testRecords = generateAllDataTypesTestRecords();

        // Publish messages to Kafka using JSON serialization
        publishAllDataTypesMessages(testRecords);

        // Wait for connector to process messages
        waitForDataInFirebolt(ALL_DATA_TYPES_TABLE_NAME, testRecords.size());

        // Verify data was written to Firebolt table
        verifyAllDataTypesRecordsInFirebolt(testRecords);
    }

    /**
     * Registers JSON schema for AllDataTypesTestRecord.
     */
    private Supplier<String> allDataTypesJsonSchema() {
        // Schema that matches the AllDataTypesTestRecord class structure
        return () -> "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
                "  \"title\": \"All Data Types Test Record\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"properties\": {\n" +
                "    \"colInteger\": {\n" +
                "      \"type\": \"integer\",\n" +
                "      \"connect.type\": \"int32\",\n" +
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
                "          \"type\": \"number\",\n" +
                "          \"connect.type\": \"bytes\",\n" +
                "          \"title\": \"org.apache.kafka.connect.data.Decimal\",\n" +
                "          \"connect.parameters\": { \"scale\": \"9\", \"connect.decimal.precision\": \"38\" }\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    \"colReal\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"connect.type\": \"float32\"}\n" +
                "      ],\n" +
                "      \"description\": \"Real field\"\n" +
                "    },\n" +
                "    \"colDoublePrecision\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"number\", \"connect.type\": \"float64\"}\n" +
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
                "    \"colTimestamp\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\"type\": \"string\", \"format\": \"date-time\"}\n" +
                "      ],\n" +
                "      \"description\": \"Timestamp field\"\n" +
                "    },\n" +
                "    \"colArrayTimestamp\": {\n" +
                "      \"oneOf\": [\n" +
                "        {\"type\": \"null\", \"title\": \"Not included\"},\n" +
                "        {\n" +
                "          \"type\": \"array\",\n" +
                "          \"items\": {\n" +
                "            \"oneOf\": [\n" +
                "              {\"type\": \"null\"},\n" +
                "              {\"type\": \"string\", \"format\": \"date-time\"}\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"description\": \"Timestamp array field with nullable elements\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"required\": [\"colInteger\"]\n" +
                "}";
    }

    private Supplier<String> allDataTypesTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"colInteger\" INTEGER NOT NULL, " +
                "\"colBigint\" BIGINT, " +
                "\"colNumeric\" NUMERIC(38,9), " +
                "\"colReal\" REAL, " +
                "\"colDoublePrecision\" DOUBLE PRECISION, " +
                "\"colBoolean\" BOOLEAN, " +
                "\"colText\" TEXT, " +
                "\"colTimestamp\" TIMESTAMP "
                + ");";
    }

    /**
     * Generates test records for all data types testing.
     */
    private List<TestRecord> generateAllDataTypesTestRecords() {
        return Arrays.asList(
                // Complete record with typical values
                aValidAllDataTypesTestRecord(1)
                        .build(),

                // Record with edge case values
                aValidAllDataTypesTestRecord(2)
                        .colBigint(Long.MAX_VALUE)
                        .colNumeric(new BigDecimal("999999999999.999"))
                        .colReal(12345.45365f)
                        .colDoublePrecision(Double.MAX_VALUE)
                        .colText("Edge Case Test Data with very long text that might exceed normal limits")
                        .colBoolean(false)
                        .colTimestamp(LocalDateTime.of(2099, 12, 31, 23, 59, 59, 999999000))
                        .build(),

                // Record with nullable values
                aValidAllDataTypesTestRecord(3)
                        .colBigint(null)
                        .colNumeric(null)
                        .colReal(null)
                        .colDoublePrecision(null)
                        .colText(null)
                        .colBoolean(null)
                        .colTimestamp(null)
                        .build(),

                // Record with geographic sample data
                aValidAllDataTypesTestRecord(4)
                        .colText("San Francisco")
                        .build(),

                // Record with variety of data patterns
                aValidAllDataTypesTestRecord(5)
                        .colBigint(-1000L)
                        .colNumeric(new BigDecimal("-1234567890.123456"))
                        .colReal(-1.5f)
                        .colDoublePrecision(-1.23456789)
                        .colText("Variety Test Data with special characters: !@#$%^&*()")
                        .colBoolean(true)
                        .colTimestamp(LocalDateTime.of(2000, 1, 1, 0, 0, 30, 0))
                        .build()
        );
    }


    /**
     * Helper method to create a valid AllDataTypesTestRecord with default values.
     */
    private TestRecord.TestRecordBuilder aValidAllDataTypesTestRecord(int colInteger) {
        return TestRecord.builder()
                // Numeric types
                .colInteger(colInteger)
                .colBigint(1000L)
                .colNumeric(new BigDecimal("123456789012.1234"))
                .colReal(1.5f)
                .colDoublePrecision(1.23456789)

                // Boolean type
                .colBoolean(true)

                // String type
                .colText("Basic Test Data")

                .colTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0));
    }

    /**
     * Publishes all data types messages to Kafka topic using JSON Schema serialization.
     */
    private void publishAllDataTypesMessages(List<TestRecord> records) throws Exception {
        for (TestRecord record : records) {
            ProducerRecord<String, TestRecord> producerRecord =
                    new ProducerRecord<>(ALL_DATA_TYPES_TOPIC_NAME, String.valueOf(record.getColInteger()), record);

            producer.send(producerRecord).get(); // Wait for each message to be sent
        }
    }

    /**
     * Verifies that all data types records were properly written to Firebolt.
     */
    private void verifyAllDataTypesRecordsInFirebolt(List<TestRecord> expectedRecords) throws SQLException {

        // Verify specific records by checking the integer column (which is unique)
        String selectQuery = "SELECT \"colInteger\", \"colBigint\", \"colNumeric\", \"colReal\", \"colDoublePrecision\", \"colBoolean\", \"colText\", " +
                "\"colTimestamp\" FROM \"" + ALL_DATA_TYPES_TABLE_NAME + "\" ORDER BY \"colInteger\"";

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(selectQuery)) {
            int recordIndex = 0;

            while (rs.next()) {
                assertTrue(recordIndex < expectedRecords.size(),
                        "More records found in database than expected");

                TestRecord expected = expectedRecords.get(recordIndex);

                // Verify key fields
                Integer actualColInteger = rs.getInt("colInteger");
                Long actualColBigint = rs.getObject("colBigint", Long.class);
                BigDecimal actualColNumeric = rs.getBigDecimal("colNumeric");
                Float actualColReal = rs.getObject("colReal", Float.class);
                Double actualColDoublePrecision = rs.getObject("colDoublePrecision", Double.class);
                String actualColText = rs.getString("colText");
                Boolean actualColBoolean = rs.getObject("colBoolean", Boolean.class);
                java.sql.Timestamp actualColTimestamp = rs.getTimestamp("colTimestamp");

                assertEquals(expected.getColInteger(), actualColInteger,
                        "ColInteger mismatch at index " + recordIndex);
                assertEquals(expected.getColBigint(), actualColBigint,
                        "ColBigint mismatch at index " + recordIndex);
                assertEqualsBigDecimal(expected.getColNumeric(), actualColNumeric, recordIndex);
                assertEquals(expected.getColReal(), actualColReal,
                        "ColReal mismatch at index " + recordIndex);
                assertEquals(expected.getColDoublePrecision(), actualColDoublePrecision,
                        "ColDoublePrecision mismatch at index " + recordIndex);
                assertEquals(expected.getColText(), actualColText,
                        "ColText mismatch at index " + recordIndex);
                assertEquals(expected.getColBoolean(), actualColBoolean,
                        "ColBoolean mismatch at index " + recordIndex);


                // Verify colTimestamp field (convert java.sql.Timestamp to LocalDateTime for comparison)
                if (actualColTimestamp != null && expected.getColTimestamp() != null) {
                    LocalDateTime actualLocalDateTime = actualColTimestamp.toLocalDateTime();
                    assertEquals(expected.getColTimestamp(), actualLocalDateTime,
                            "ColTimestamp mismatch at index " + recordIndex);
                }

                recordIndex++;
            }

            assertEquals(expectedRecords.size(), recordIndex,
                    "Expected " + expectedRecords.size() + " records but processed " + recordIndex);
        }
    }



    /**
     * Test record class that mirrors the structure of the all data types test table.
     * This class provides a Java object representation of all Firebolt data types
     * for use in integration tests.
     */
    @Data
    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestRecord {
        // Numeric types
        private Integer colInteger;           // colInteger INTEGER NOT NULL
        private Long colBigint;              // colBigint BIGINT

        private BigDecimal colNumeric;       // colNumeric NUMERIC(38,9) - serialized as string to preserve precision
        private Float colReal;               // colReal REAL
        private Double colDoublePrecision;   // colDoublePrecision DOUBLE PRECISION

        // Boolean type
        private Boolean colBoolean;          // colBoolean BOOLEAN

        // String type
        private String colText;              // colText TEXT

        private LocalDateTime colTimestamp;  // colTimestamp TIMESTAMP

    }
}
