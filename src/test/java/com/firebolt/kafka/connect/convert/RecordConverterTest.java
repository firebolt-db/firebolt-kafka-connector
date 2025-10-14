package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class RecordConverterTest {

    @Mock
    private SinkConfig mockConfig;

    @Mock
    private SinkRecord mockSinkRecord;

    private TestRecordConverter converter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new TestRecordConverter(mockConfig);
    }

    @Test
    void testConstructorWithConfig() {
        TestRecordConverter testConverter = new TestRecordConverter(mockConfig);
        assertNotNull(testConverter);
        assertEquals(mockConfig, testConverter.getConfig());
    }

    @Test
    void testConvertWithValidRecord() throws RecordConversionException {
        // Setup mock behavior
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(1);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        // Configure test converter to return test data
        Map<String, SchemaKafkaMessageColumnValue> testColumnValues = new HashMap<>();
        testColumnValues.put("field1", SchemaKafkaMessageColumnValue.builder().value("value1").build());
        testColumnValues.put("field2", SchemaKafkaMessageColumnValue.builder().value(42).build());
        converter.setTestColumnValues(testColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals("test_table", result.getTableName());
        assertEquals("test-topic", result.getTopic());
        assertEquals(1, result.getPartition());
        assertEquals(100L, result.getOffset());
        assertEquals(1234567890L, result.getTimestamp());
        assertEquals(testColumnValues, result.getColumnValues());
    }

    @Test
    void testConvertWithNullPartition() throws RecordConversionException {
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(null); // null partition
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        Map<String, SchemaKafkaMessageColumnValue> testColumnValues = new HashMap<>();
        converter.setTestColumnValues(testColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(-1, result.getPartition()); // Should default to -1
    }

    @Test
    void testConvertWithNullTimestamp() throws RecordConversionException {
        long beforeConversion = System.currentTimeMillis();
        
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(1);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(null); // null timestamp

        Map<String, SchemaKafkaMessageColumnValue> testColumnValues = new HashMap<>();
        converter.setTestColumnValues(testColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        long afterConversion = System.currentTimeMillis();
        assertTrue(result.getTimestamp() >= beforeConversion && result.getTimestamp() <= afterConversion,
                "Timestamp should be set to current system time when null");
    }

    @Test
    void testConvertWithNoTableMappingThrowsException() {
        when(mockConfig.getTableNameForTopic("unmapped-topic")).thenReturn(null);
        when(mockConfig.getTopicToTableMapping()).thenReturn("other-topic:other_table");
        when(mockSinkRecord.topic()).thenReturn("unmapped-topic");

        RecordConversionException exception = assertThrows(RecordConversionException.class, () -> {
            converter.convert(mockSinkRecord);
        });

        assertEquals("No table mapping found for topic: unmapped-topic", exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
        "test-topic-1, test_table_1",
        "test-topic-2, test_table_2",
        "user-events, user_events_table",
        "order-data, orders_table",
        "metrics, metrics_table"
    })
    void testConvertWithDifferentTopicMappings(String topic, String expectedTable) throws RecordConversionException {
        when(mockConfig.getTableNameForTopic(topic)).thenReturn(expectedTable);
        when(mockSinkRecord.topic()).thenReturn(topic);
        when(mockSinkRecord.kafkaPartition()).thenReturn(0);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        Map<String, SchemaKafkaMessageColumnValue> testColumnValues = new HashMap<>();
        converter.setTestColumnValues(testColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(expectedTable, result.getTableName());
        assertEquals(topic, result.getTopic());
    }

    @ParameterizedTest
    @CsvSource({
        "0, 100",
        "1, 200",
        "5, 500",
        "10, 1000",
        "100, 50000"
    })
    void testConvertWithDifferentPartitionsAndOffsets(int partition, long offset) throws RecordConversionException {
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(partition);
        when(mockSinkRecord.kafkaOffset()).thenReturn(offset);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        Map<String, SchemaKafkaMessageColumnValue> testColumnValues = new HashMap<>();
        converter.setTestColumnValues(testColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(partition, result.getPartition());
        assertEquals(offset, result.getOffset());
    }

    @Test
    void testHandleNullValueReturnsEmptyMap() {
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(1);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);

        Map<String, ? extends SchemaKafkaMessageColumnValue> result = converter.testHandleNullValue(mockSinkRecord);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testConvertRecordValueCalledCorrectly() throws RecordConversionException {
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(1);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        Map<String, SchemaKafkaMessageColumnValue> expectedColumnValues = new HashMap<>();
        expectedColumnValues.put("test_field", SchemaKafkaMessageColumnValue.builder().value("test_value").build());
        converter.setTestColumnValues(expectedColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertTrue(converter.wasConvertRecordValueCalled());
        assertEquals(expectedColumnValues, result.getColumnValues());
    }

    @Test
    void testConvertRecordValueThrowsException() {
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");

        converter.setShouldThrowException(true);

        RecordConversionException exception = assertThrows(RecordConversionException.class, () -> {
            converter.convert(mockSinkRecord);
        });

        assertEquals("Test conversion error", exception.getMessage());
    }

    @Test
    void testConvertWithEmptyColumnValues() throws RecordConversionException {
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(1);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        Map<String, SchemaKafkaMessageColumnValue> emptyColumnValues = new HashMap<>();
        converter.setTestColumnValues(emptyColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        assertNotNull(result.getColumnValues());
        assertTrue(result.getColumnValues().isEmpty());
    }

    @Test
    void testConvertWithComplexColumnValues() throws RecordConversionException {
        when(mockConfig.getTableNameForTopic("test-topic")).thenReturn("test_table");
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(1);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(1234567890L);

        Map<String, SchemaKafkaMessageColumnValue> complexColumnValues = new HashMap<>();
        complexColumnValues.put("string_field", SchemaKafkaMessageColumnValue.builder().value("test_string").build());
        complexColumnValues.put("int_field", SchemaKafkaMessageColumnValue.builder().value(42).build());
        complexColumnValues.put("double_field", SchemaKafkaMessageColumnValue.builder().value(3.14).build());
        complexColumnValues.put("boolean_field", SchemaKafkaMessageColumnValue.builder().value(true).build());
        complexColumnValues.put("null_field", null);
        
        converter.setTestColumnValues(complexColumnValues);

        FireboltRecord result = converter.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(complexColumnValues, result.getColumnValues());
    }

    /**
     * Test implementation of RecordConverter for testing purposes.
     * This allows us to test the concrete methods in the abstract base class.
     */
    private static class TestRecordConverter extends RecordConverter {
        private Map<String, SchemaKafkaMessageColumnValue> testColumnValues = new HashMap<>();
        private boolean convertRecordValueCalled = false;
        private boolean shouldThrowException = false;

        public TestRecordConverter(SinkConfig config) {
            super(config);
        }

        public SinkConfig getConfig() {
            return this.config;
        }

        public void setTestColumnValues(Map<String, SchemaKafkaMessageColumnValue> columnValues) {
            this.testColumnValues = columnValues;
        }

        public void setShouldThrowException(boolean shouldThrow) {
            this.shouldThrowException = shouldThrow;
        }

        public boolean wasConvertRecordValueCalled() {
            return convertRecordValueCalled;
        }

        public Map<String, ? extends SchemaKafkaMessageColumnValue> testHandleNullValue(SinkRecord record) {
            return handleNullValue(record);
        }

        @Override
        protected Map<String, SchemaKafkaMessageColumnValue> convertRecordValue(SinkRecord record) throws RecordConversionException {
            convertRecordValueCalled = true;
            if (shouldThrowException) {
                throw new RecordConversionException("Test conversion error");
            }
            return testColumnValues;
        }

        @Override
        public boolean canHandle(SinkRecord record) {
            return true; // For testing, always return true
        }

        @Override
        public String getDescription() {
            return "Test Record Converter for unit testing";
        }
    }
} 