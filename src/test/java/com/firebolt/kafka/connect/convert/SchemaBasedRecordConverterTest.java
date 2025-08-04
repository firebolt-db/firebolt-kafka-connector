package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class
SchemaBasedRecordConverterTest {

    @Mock
    private SinkConfig mockConfig;

    @Mock
    private SinkRecord mockSinkRecord;

    @Mock
    private Schema mockSchema;

    @Mock
    private Struct mockStruct;

    @Mock
    private Field mockField;

    @Mock
    private Schema mockFieldSchema;

    private SchemaBasedRecordConverter converter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemaBasedRecordConverter(mockConfig);
    }

    @Test
    void testConstructorWithConfig() {
        SchemaBasedRecordConverter testConverter = new SchemaBasedRecordConverter(mockConfig);
        assertNotNull(testConverter);
    }

    @Test
    void testGetDescription() {
        String description = converter.getDescription();
        assertEquals("Handles records with embedded schemas (typically Struct values from Avro/JSON+Schema)", description);
    }

    @Test
    void testCanHandleValidStructRecord() {
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(mockStruct);

        boolean result = converter.canHandle(mockSinkRecord);

        assertTrue(result);
    }

    @Test
    void testCanHandleRecordWithNullSchema() {
        when(mockSinkRecord.valueSchema()).thenReturn(null);
        when(mockSinkRecord.value()).thenReturn(mockStruct);

        boolean result = converter.canHandle(mockSinkRecord);

        assertFalse(result);
    }

    @Test
    void testCanHandleRecordWithNonStructValue() {
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn("not a struct");

        boolean result = converter.canHandle(mockSinkRecord);

        assertFalse(result);
    }

    @Test
    void testCanHandleRecordWithNullValue() {
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.value()).thenReturn(null);

        boolean result = converter.canHandle(mockSinkRecord);

        assertFalse(result);
    }

    @ParameterizedTest
    @CsvSource({
        "STRING, 'hello world'",
        "INT32, 42",
        "INT64, 12345678901234",
        "FLOAT32, 3.14",
        "FLOAT64, 2.718281828",
        "BOOLEAN, true"
    })
    void testConvertRecordValueWithDifferentFieldTypes(String schemaType, String fieldValue) throws RecordConversionException {
        // Setup mock behavior
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        
        // Setup field with specific type
        when(mockField.name()).thenReturn("testField");
        when(mockField.schema()).thenReturn(mockFieldSchema);
        when(mockFieldSchema.type()).thenReturn(Schema.Type.valueOf(schemaType));
        when(mockFieldSchema.parameters()).thenReturn(null);
        when(mockSchema.fields()).thenReturn(Collections.singletonList(mockField));
        
        // Setup struct to return the field value
        Object value = convertStringToType(fieldValue, schemaType);
        when(mockStruct.get("testField")).thenReturn(value);

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("testField"));
        
        KafkaMessageColumnValue columnValue = result.get("testField");
        assertNotNull(columnValue);
        assertEquals(value, columnValue.getValue());
        assertEquals(Schema.Type.valueOf(schemaType), columnValue.getSchemaType());
    }

    @Test
    void testConvertRecordValueWithEmptyStruct() throws RecordConversionException {
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSchema.fields()).thenReturn(Collections.emptyList());

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testConvertRecordValueWithMultipleFields() throws RecordConversionException {
        // Setup multiple mock fields
        Field mockField1 = org.mockito.Mockito.mock(Field.class);
        Field mockField2 = org.mockito.Mockito.mock(Field.class);
        Field mockField3 = org.mockito.Mockito.mock(Field.class);
        
        Schema mockSchema1 = org.mockito.Mockito.mock(Schema.class);
        Schema mockSchema2 = org.mockito.Mockito.mock(Schema.class);
        Schema mockSchema3 = org.mockito.Mockito.mock(Schema.class);

        when(mockField1.name()).thenReturn("field1");
        when(mockField1.schema()).thenReturn(mockSchema1);
        when(mockSchema1.type()).thenReturn(Schema.Type.STRING);
        when(mockSchema1.parameters()).thenReturn(null);
        
        when(mockField2.name()).thenReturn("field2");
        when(mockField2.schema()).thenReturn(mockSchema2);
        when(mockSchema2.type()).thenReturn(Schema.Type.INT32);
        when(mockSchema2.parameters()).thenReturn(null);
        
        when(mockField3.name()).thenReturn("field3");
        when(mockField3.schema()).thenReturn(mockSchema3);
        when(mockSchema3.type()).thenReturn(Schema.Type.BOOLEAN);
        when(mockSchema3.parameters()).thenReturn(null);

        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSchema.fields()).thenReturn(Arrays.asList(mockField1, mockField2, mockField3));

        // Setup struct field values
        when(mockStruct.get("field1")).thenReturn("test_string");
        when(mockStruct.get("field2")).thenReturn(123);
        when(mockStruct.get("field3")).thenReturn(true);

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        assertEquals(3, result.size());
        
        assertEquals("test_string", result.get("field1").getValue());
        assertEquals(Schema.Type.STRING, result.get("field1").getSchemaType());
        
        assertEquals(123, result.get("field2").getValue());
        assertEquals(Schema.Type.INT32, result.get("field2").getSchemaType());
        
        assertEquals(true, result.get("field3").getValue());
        assertEquals(Schema.Type.BOOLEAN, result.get("field3").getSchemaType());
    }

    @Test
    void testConvertRecordValueWithNullFieldValues() throws RecordConversionException {
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        
        when(mockField.name()).thenReturn("nullField");
        when(mockField.schema()).thenReturn(mockFieldSchema);
        when(mockSchema.fields()).thenReturn(Collections.singletonList(mockField));
        when(mockStruct.get("nullField")).thenReturn(null);

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertNull(result.get("nullField"));
    }

    @Test
    void testConvertRecordValueWithNullRecord() throws RecordConversionException {
        when(mockSinkRecord.value()).thenReturn(null);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        when(mockSinkRecord.topic()).thenReturn("test-topic");
        when(mockSinkRecord.kafkaPartition()).thenReturn(0);
        when(mockSinkRecord.kafkaOffset()).thenReturn(100L);

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        assertTrue(result.isEmpty()); // handleNullValue returns empty map
    }

    @Test
    void testConvertRecordValueWithNonStructValueThrowsException() {
        when(mockSinkRecord.value()).thenReturn("not a struct");
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);

        RecordConversionException exception = assertThrows(RecordConversionException.class, () -> {
            converter.convertRecordValue(mockSinkRecord);
        });

        assertEquals("Expected Struct value with schema, but got String", exception.getMessage());
    }

    @Test
    void testConvertRecordValueWithSchemaParameters() throws RecordConversionException {
        Map<String, String> schemaParams = new HashMap<>();
        schemaParams.put("precision", "10");
        schemaParams.put("scale", "2");

        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        
        when(mockField.name()).thenReturn("decimalField");
        when(mockField.schema()).thenReturn(mockFieldSchema);
        when(mockFieldSchema.type()).thenReturn(Schema.Type.BYTES);
        when(mockFieldSchema.parameters()).thenReturn(schemaParams);
        when(mockSchema.fields()).thenReturn(Collections.singletonList(mockField));
        when(mockStruct.get("decimalField")).thenReturn(new byte[]{1, 2, 3});

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        KafkaMessageColumnValue columnValue = result.get("decimalField");
        assertNotNull(columnValue);
        assertEquals(schemaParams, columnValue.getSchemaTypeParams());
    }

    @ParameterizedTest
    @CsvSource({
        "field1",
        "field_with_underscore",
        "fieldWithCamelCase",
        "field-with-dashes",
        "field123",
        "FIELD_UPPERCASE"
    })
    void testConvertRecordValueWithDifferentFieldNames(String fieldName) throws RecordConversionException {
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        
        when(mockField.name()).thenReturn(fieldName);
        when(mockField.schema()).thenReturn(mockFieldSchema);
        when(mockFieldSchema.type()).thenReturn(Schema.Type.STRING);
        when(mockFieldSchema.parameters()).thenReturn(null);
        when(mockSchema.fields()).thenReturn(Collections.singletonList(mockField));
        when(mockStruct.get(fieldName)).thenReturn("test_value");

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        assertTrue(result.containsKey(fieldName));
        assertEquals("test_value", result.get(fieldName).getValue());
    }

    @Test
    void testConvertRecordValueWithArrayField() throws RecordConversionException {
        List<String> arrayValue = Arrays.asList("item1", "item2", "item3");
        
        // Setup mock for array element schema
        Schema mockArrayElementSchema = org.mockito.Mockito.mock(Schema.class);
        when(mockArrayElementSchema.type()).thenReturn(Schema.Type.STRING);
        
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        
        when(mockField.name()).thenReturn("arrayField");
        when(mockField.schema()).thenReturn(mockFieldSchema);
        when(mockFieldSchema.type()).thenReturn(Schema.Type.ARRAY);
        when(mockFieldSchema.parameters()).thenReturn(null);
        when(mockFieldSchema.valueSchema()).thenReturn(mockArrayElementSchema); // Add missing mock setup
        when(mockSchema.fields()).thenReturn(Collections.singletonList(mockField));
        when(mockStruct.get("arrayField")).thenReturn(arrayValue);

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        KafkaMessageColumnValue columnValue = result.get("arrayField");
        assertNotNull(columnValue);
        assertEquals(arrayValue, columnValue.getValue());
        assertEquals(Schema.Type.ARRAY, columnValue.getSchemaType());
        assertEquals(Schema.Type.STRING, columnValue.getSchemaSubType()); // Also verify the subtype is set correctly
    }

    @Test
    void testConvertRecordValueWithTimestampArrayField() throws RecordConversionException {
        List<Long> timestampArrayValue = Arrays.asList(1609459200000L, 1609459260000L, 1609459320000L);
        
        // Setup mock for array element schema (INT64 for timestamp values)
        Schema mockArrayElementSchema = org.mockito.Mockito.mock(Schema.class);
        when(mockArrayElementSchema.type()).thenReturn(Schema.Type.INT64);
        
        when(mockSinkRecord.value()).thenReturn(mockStruct);
        when(mockSinkRecord.valueSchema()).thenReturn(mockSchema);
        
        when(mockField.name()).thenReturn("timestampArrayField");
        when(mockField.schema()).thenReturn(mockFieldSchema);
        when(mockFieldSchema.type()).thenReturn(Schema.Type.ARRAY);
        when(mockFieldSchema.parameters()).thenReturn(null);
        when(mockFieldSchema.valueSchema()).thenReturn(mockArrayElementSchema);
        when(mockSchema.fields()).thenReturn(Collections.singletonList(mockField));
        when(mockStruct.get("timestampArrayField")).thenReturn(timestampArrayValue);

        Map<String, KafkaMessageColumnValue> result = converter.convertRecordValue(mockSinkRecord);

        assertNotNull(result);
        KafkaMessageColumnValue columnValue = result.get("timestampArrayField");
        assertNotNull(columnValue);
        assertEquals(timestampArrayValue, columnValue.getValue());
        assertEquals(Schema.Type.ARRAY, columnValue.getSchemaType());
        assertEquals(Schema.Type.INT64, columnValue.getSchemaSubType()); // Verify timestamp array subtype
    }



    private Object convertStringToType(String value, String schemaType) {
        switch (schemaType) {
            case "STRING":
                return value;
            case "INT32":
                return Integer.parseInt(value);
            case "INT64":
                return Long.parseLong(value);
            case "FLOAT32":
                return Float.parseFloat(value);
            case "FLOAT64":
                return Double.parseDouble(value);
            case "BOOLEAN":
                return Boolean.parseBoolean(value);
            default:
                return value;
        }
    }
} 