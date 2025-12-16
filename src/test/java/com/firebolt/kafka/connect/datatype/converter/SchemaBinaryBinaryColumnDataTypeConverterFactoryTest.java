package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaArrayDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessBigIntDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessBooleanDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaByteaDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaDateDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaDecimalDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessDoubleDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaIntegerDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaRealDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessTextDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTimestampDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTimestamptzDataTypeConverter;
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

public class SchemaBinaryBinaryColumnDataTypeConverterFactoryTest {

    @Mock
    private SchemaIntegerDataTypeConverter mockIntegerDataTypeConverter;

    @Mock
    private SchemaArrayDataTypeConverter mockArrayDataTypeConverter;

    @Mock
    private SchemaTimestampDataTypeConverter mockTimestampDataTypeConverter;

    @Mock
    private SchemaTimestamptzDataTypeConverter mockTimestamptzDataTypeConverter;

    @Mock
    private SchemaDateDataTypeConverter mockDateDataTypeConverter;

    @Mock
    private SchemaDecimalDataTypeConverter mockDecimalDataTypeConverter;

    @Mock
    private SchemalessBigIntDataTypeConverter mockBigIntDataTypeConverter;

    @Mock
    private SchemaRealDataTypeConverter mockRealDataTypeConverter;

    @Mock
    private SchemalessDoubleDataTypeConverter mockDoubleDataTypeConverter;

    @Mock
    private SchemalessTextDataTypeConverter mockTextDataTypeConverter;

    @Mock
    private SchemaByteaDataTypeConverter mockByteaDataTypeConverter;

    @Mock
    private SchemalessBooleanDataTypeConverter mockBooleanDataTypeConverter;

    private ColumnDataTypeConverterFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new SchemaColumnTypeConverterFactory(mockIntegerDataTypeConverter, mockArrayDataTypeConverter, mockTimestampDataTypeConverter,
                mockTimestamptzDataTypeConverter, mockDecimalDataTypeConverter, mockBigIntDataTypeConverter, mockRealDataTypeConverter,
                mockDoubleDataTypeConverter, mockTextDataTypeConverter, mockDateDataTypeConverter, mockByteaDataTypeConverter, mockBooleanDataTypeConverter);
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
        "array(real)",
        "array(boolean)",
        "ARRAY(TEXT)",
        "ARRAY(INTEGER)",
        "Array(Text)",
        "Array(BigInt)",
        "array(array(text))",
        "array("
    })
    void testGetConverterForArrayTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 2003, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockArrayDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "array(bytea)",
        "ARRAY(BYTEA)",
        "Array(Bytea)",
        "Array(BYTEA)"
    })
    void testGetConverterForByteaArrayTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 2003, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockArrayDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "array(timestamptz)",
        "ARRAY(TIMESTAMPTZ)",
        "Array(Timestamptz)",
        "Array(TIMESTAMPTZ)"
    })
    void testGetConverterForTimestamptzArrayTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 2003, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockArrayDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "timestamp",
        "TIMESTAMP",
        "Timestamp"
    })
    void testGetConverterForTimestampTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 93, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockTimestampDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "timestamptz",
        "TIMESTAMPTZ",
        "TimestampTz",
        "TimeStampTz"
    })
    void testGetConverterForTimestamptzTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 2014, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockTimestamptzDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "numeric",
        "decimal",
        "NUMERIC",
        "DECIMAL",
        "Numeric",
        "Decimal"
    })
    void testGetConverterForDecimalTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 2, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockDecimalDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "bigint",
        "int8",
        "long",
        "BIGINT",
        "INT8",
        "LONG",
        "BigInt",
        "Int8",
        "Long"
    })
    void testGetConverterForBigintTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 8, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockBigIntDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "real",
        "float4",
        "REAL",
        "FLOAT4",
        "Real",
        "Float4"
    })
    void testGetConverterForRealTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 7, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockRealDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "double precision",
        "double",
        "float",
        "float8",
        "float(p)",
        "DOUBLE PRECISION",
        "DOUBLE",
        "FLOAT",
        "FLOAT8",
        "FLOAT(P)",
        "Double Precision",
        "Double",
        "Float",
        "Float8",
        "Float(P)"
    })
    void testGetConverterForDoubleTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 8, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockDoubleDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "text",
        "TEXT",
        "Text",
        "TeXt"
    })
    void testGetConverterForTextTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 12, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockTextDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "date",
        "DATE",
        "Date",
        "DaTe"
    })
    void testGetConverterForDateTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 91, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockDateDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "bytea",
        "BYTEA",
        "Bytea",
        "ByTeA"
    })
    void testGetConverterForByteaTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, -2, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockByteaDataTypeConverter, result);
    }

    @ParameterizedTest
    @CsvSource({
        "boolean",
        "bool",
        "BOOLEAN",
        "BOOL",
        "Boolean",
        "Bool",
        "BoOlEaN",
        "BoOl"
    })
    void testGetConverterForBooleanTypes(String dataType) {
        TableSchema.Column column = new TableSchema.Column("test_column", dataType, 16, true);
        
        ColumnDataTypeConverter result = factory.getConverter(column);
        
        assertSame(mockBooleanDataTypeConverter, result);
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
    void testConstructorWithMocks() {
        ColumnDataTypeConverterFactory testFactory = new SchemaColumnTypeConverterFactory(
            mockIntegerDataTypeConverter, mockArrayDataTypeConverter, mockTimestampDataTypeConverter, mockTimestamptzDataTypeConverter, mockDecimalDataTypeConverter,
                mockBigIntDataTypeConverter, mockRealDataTypeConverter, mockDoubleDataTypeConverter, mockTextDataTypeConverter, mockDateDataTypeConverter, mockByteaDataTypeConverter, mockBooleanDataTypeConverter);
        
        assertNotNull(testFactory);
        
        TableSchema.Column intColumn = new TableSchema.Column("int_col", "integer", 4, true);
        TableSchema.Column arrayColumn = new TableSchema.Column("array_col", "array(text)", 2003, true);
        TableSchema.Column timestampColumn = new TableSchema.Column("timestamp_col", "timestamp", 93, true);
        TableSchema.Column dateColumn = new TableSchema.Column("date_col", "date", 91, true);
        TableSchema.Column bigintColumn = new TableSchema.Column("bigint_col", "bigint", 8, true);
        TableSchema.Column realColumn = new TableSchema.Column("real_col", "real", 7, true);
        TableSchema.Column doubleColumn = new TableSchema.Column("double_col", "double", 8, true);
        TableSchema.Column textColumn = new TableSchema.Column("text_col", "text", 12, true);
        TableSchema.Column byteaColumn = new TableSchema.Column("bytea_col", "bytea", -2, true);
        TableSchema.Column booleanColumn = new TableSchema.Column("boolean_col", "boolean", 16, true);
        
        assertSame(mockIntegerDataTypeConverter, testFactory.getConverter(intColumn));
        assertSame(mockArrayDataTypeConverter, testFactory.getConverter(arrayColumn));
        assertSame(mockTimestampDataTypeConverter, testFactory.getConverter(timestampColumn));
        assertSame(mockDateDataTypeConverter, testFactory.getConverter(dateColumn));
        assertSame(mockBigIntDataTypeConverter, testFactory.getConverter(bigintColumn));
        assertSame(mockRealDataTypeConverter, testFactory.getConverter(realColumn));
        assertSame(mockDoubleDataTypeConverter, testFactory.getConverter(doubleColumn));
        assertSame(mockTextDataTypeConverter, testFactory.getConverter(textColumn));
        assertSame(mockByteaDataTypeConverter, testFactory.getConverter(byteaColumn));
        assertSame(mockBooleanDataTypeConverter, testFactory.getConverter(booleanColumn));
    }
} 