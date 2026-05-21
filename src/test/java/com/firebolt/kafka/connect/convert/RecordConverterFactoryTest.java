package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordConverterFactoryTest {

    @Mock
    private SinkConfig mockConfig;

    @Mock
    private SinkRecord mockSinkRecord;

    @Mock
    private Schema mockSchema;

    @Mock
    private Struct mockStruct;

    private RecordConverterFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Setup basic config behavior
        when(mockConfig.getTableNameForTopic(any(String.class))).thenReturn("test_table");
        when(mockConfig.getTopicToTableMapping()).thenReturn("test-topic:test_table");
        
        factory = new RecordConverterFactory(mockConfig);
    }

    @Test
    void testConstructorCreatesConverters() {
        RecordConverterFactory testFactory = new RecordConverterFactory(mockConfig);
        
        assertNotNull(testFactory);
        // Factory should be created successfully with at least one converter (SchemaBasedRecordConverter)
    }

    @Test
    void testConvertWithValidSchemaBasedRecord() throws RecordConversionException {
        // Setup a record that SchemaBasedRecordConverter can handle
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(0);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(System.currentTimeMillis());
        
        // Setup struct field access
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());

        AbstractFireboltRecord result = factory.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals("test_table", result.getTableName());
        assertEquals("test-topic", result.getTopic());
        assertEquals(0, result.getPartition());
        assertEquals(100L, result.getOffset());
    }

    @Test
    void testConvertWithRecordNoConverterCanHandle() {
        // No converter handles null-schema + non-Map value; should throw a descriptive exception.
        when(mockSinkRecord.valueSchema()).thenReturn(null);
        when(mockSinkRecord.value()).thenReturn("plain string value");
        when(mockSinkRecord.topic()).thenReturn("test-topic");

        RecordConversionException ex = assertThrows(RecordConversionException.class, () -> {
            factory.convert(mockSinkRecord);
        });
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains("No converter found"));
    }

    @Test
    void testConvertWithNullRecord() {
        assertThrows(NullPointerException.class, () -> {
            factory.convert(null);
        });
    }

    @Test
    void testConvertWithNoTableMappingThrowsException() {
        // Setup a valid record but no table mapping
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn("unmapped-topic");
        when(mockConfig.getTableNameForTopic("unmapped-topic")).thenReturn(null);
        when(mockConfig.getTopicToTableMapping()).thenReturn("other-topic:other_table");
        
        // Setup struct field access
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());

        RecordConversionException exception = assertThrows(RecordConversionException.class, () -> {
            factory.convert(mockSinkRecord);
        });

        assertEquals("No table mapping found for topic: unmapped-topic", exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
        "test-topic-1, test_table_1",
        "test-topic-2, test_table_2",
        "user-events, user_events_table",
        "order-data, orders_table"
    })
    void testConvertWithDifferentTopicMappings(String topic, String expectedTable) throws RecordConversionException {
        // Setup record for each topic
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn(topic);
        when(mockSinkRecord.kafkaPartition()).thenReturn(0);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(System.currentTimeMillis());
        when(mockConfig.getTableNameForTopic(topic)).thenReturn(expectedTable);
        
        // Setup struct field access
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());

        AbstractFireboltRecord result = factory.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(expectedTable, result.getTableName());
        assertEquals(topic, result.getTopic());
    }

    @Test
    void testConvertWithNullPartition() throws RecordConversionException {
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(null); // null partition
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(System.currentTimeMillis());
        
        // Setup struct field access
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());

        AbstractFireboltRecord result = factory.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(-1, result.getPartition()); // Should default to -1
    }

    @Test
    void testConvertWithNullTimestamp() throws RecordConversionException {
        long beforeConversion = System.currentTimeMillis();
        
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(0);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(null); // null timestamp
        
        // Setup struct field access
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());

        AbstractFireboltRecord result = factory.convert(mockSinkRecord);

        assertNotNull(result);
        // Should use current system time when timestamp is null
        long afterConversion = System.currentTimeMillis();
        assertTrue(result.getTimestamp() >= beforeConversion && result.getTimestamp() <= afterConversion,
                "Timestamp should be set to current system time when null");
    }

    @Test
    void testConvertWithSchemalessRecord() {
        // Test record without schema (should not be handled by SchemaBasedRecordConverter)
        when(mockSinkRecord.valueSchema()).thenReturn(null);
        when(mockSinkRecord.value()).thenReturn(Map.of("key", "value"));
        when(mockSinkRecord.topic()).thenReturn("test-topic");

        // With SchemalessBasedRecordConverter registered, conversion should succeed
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.convert(mockSinkRecord));
    }

    @Test
    void testConvertWithNonStructValueThrowsRecordConversionException() {
        // Schema-backed record with a non-Struct value: no converter matches → descriptive exception.
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn("not a struct");
        when(mockSinkRecord.topic()).thenReturn("test-topic");

        RecordConversionException ex = assertThrows(RecordConversionException.class, () -> {
            factory.convert(mockSinkRecord);
        });
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains("No converter found"));
        assertTrue(ex.getMessage().contains("String")); // value type in message
    }

    @Test
    void testConvertWithNullValueThrowsRecordConversionException() {
        // Schema-backed record with null value: no converter matches → descriptive exception.
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(null);
        when(mockSinkRecord.topic()).thenReturn("test-topic");

        RecordConversionException ex = assertThrows(RecordConversionException.class, () -> {
            factory.convert(mockSinkRecord);
        });
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains("No converter found"));
        assertTrue(ex.getMessage().contains("null")); // value type in message
    }

    @Test
    void testConvertWithStructContainingNullFields() throws RecordConversionException {
        // Test record with valid Struct that contains null field values
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(0);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);
        when(mockSinkRecord.timestamp()).thenReturn(System.currentTimeMillis());
        
        // Setup struct with null field values
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());
        when(mockStruct.get(any(String.class))).thenReturn(null);

        AbstractFireboltRecord result = factory.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals("test_table", result.getTableName());
        assertNotNull(result.getColumnNames());
    }

    @ParameterizedTest
    @CsvSource({
        "0, 100",
        "1, 200", 
        "5, 500",
        "10, 1000"
    })
    void testConvertWithDifferentPartitionsAndOffsets(int partition, long offset) throws RecordConversionException {
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(partition);
        when(mockSinkRecord.kafkaOffset()).thenReturn(offset);
        when(mockSinkRecord.timestamp()).thenReturn(System.currentTimeMillis());
        
        // Setup struct field access
        when(mockSchema.fields()).thenReturn(java.util.Collections.emptyList());

        AbstractFireboltRecord result = factory.convert(mockSinkRecord);

        assertNotNull(result);
        assertEquals(partition, result.getPartition());
        assertEquals(offset, result.getOffset());
    }

} 