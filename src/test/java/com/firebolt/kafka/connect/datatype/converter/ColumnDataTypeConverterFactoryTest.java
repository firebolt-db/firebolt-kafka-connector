package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ColumnDataTypeConverterFactoryTest {

    @Mock
    private IntegerDataTypeConverter mockIntegerDataTypeConverter;

    @Mock
    private ArrayDataTypeConverter mockArrayDataTypeConverter;

    private ColumnDataTypeConverterFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new ColumnDataTypeConverterFactory(mockIntegerDataTypeConverter, mockArrayDataTypeConverter);
    }

    @ParameterizedTest
    @CsvSource({
        "integer",
        "int",
        "int4",
        "INTEGER",
        "INT",
        "INT4"
    })
    void testGetConverterForIntegerTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 4, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockIntegerDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "array(text)",
        "array(integer)",
        "array(bigint)",
        "ARRAY(TEXT)",
        "ARRAY(INTEGER)",
        "Array(Text)"
    })
    void testGetConverterForArrayTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 2003, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockArrayDataTypeConverter, result);
    }

    @Test
    void testGetConverterThrowsExceptionForUnsupportedType() {
        TableSchema.Column column = new TableSchema.Column("test_column", "unsupported_type", 12, true);
        
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> factory.getConverter(column));
        
        assertEquals("Invalid column type name: unsupported_type", exception.getMessage());
    }

    @Test
    void testGetConverterThrowsExceptionForNullType() {
        TableSchema.Column column = new TableSchema.Column("test_column", null, 12, true);
        
        assertThrows(RuntimeException.class, () -> factory.getConverter(column));
    }

    @Test
    void testGetInstanceReturnsSameInstance() {
        ColumnDataTypeConverterFactory instance1 = ColumnDataTypeConverterFactory.getInstance();
        ColumnDataTypeConverterFactory instance2 = ColumnDataTypeConverterFactory.getInstance();
        
        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    @Test
    void testConstructorWithMocks() {
        ColumnDataTypeConverterFactory testFactory = new ColumnDataTypeConverterFactory(
            mockIntegerDataTypeConverter, mockArrayDataTypeConverter);
        
        assertNotNull(testFactory);
        
        TableSchema.Column intColumn = new TableSchema.Column("int_col", "integer", 4, true);
        TableSchema.Column arrayColumn = new TableSchema.Column("array_col", "array(text)", 2003, true);
        
        assertSame(mockIntegerDataTypeConverter, testFactory.getConverter(intColumn));
        assertSame(mockArrayDataTypeConverter, testFactory.getConverter(arrayColumn));
    }
} 