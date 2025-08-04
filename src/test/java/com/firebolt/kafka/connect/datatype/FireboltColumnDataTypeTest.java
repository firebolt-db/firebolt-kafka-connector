package com.firebolt.kafka.connect.datatype;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FireboltColumnDataTypeTest {

    @ParameterizedTest
    @CsvSource({
        "integer",
        "int", 
        "int4",
        "INTEGER",
        "INT",
        "INT4",
        "Integer",
        "Int",
        "Int4"
    })
    void testFromStringReturnsIntegerForValidIntegerTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.INTEGER, result);
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
    void testFromStringReturnsArrayForValidArrayTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.ARRAY, result);
    }

    @ParameterizedTest
    @CsvSource({
        "timestamp",
        "TIMESTAMP",
        "Timestamp",
        "TimEsTaMp"
    })
    void testFromStringReturnsTimestampForValidTimestampTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.TIMESTAMP, result);
    }

    @ParameterizedTest
    @CsvSource({
        "numeric",
        "decimal",
        "NUMERIC",
        "DECIMAL",
        "Numeric",
        "Decimal",
        "NuMeRiC",
        "DeCiMaL"
    })
    void testFromStringReturnsDecimalForValidDecimalTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.DECIMAL, result);
    }

    @ParameterizedTest
    @CsvSource({
        "timestamptz",
        "TIMESTAMPTZ",
        "TimestampTz",
        "TimeStampTz",
        "TIMESTAMPTZ",
        "TimEsTaMpTz"
    })
    void testFromStringReturnsTimestamptzForValidTimestamptzTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.TIMESTAMPTZ, result);
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
        "Long",
        "BiGiNt",
        "InT8",
        "LoNg"
    })
    void testFromStringReturnsBigintForValidBigintTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.BIGINT, result);
    }

    @ParameterizedTest
    @CsvSource({
        "real",
        "float4",
        "REAL",
        "FLOAT4",
        "Real",
        "Float4",
        "ReAl",
        "FlOaT4"
    })
    void testFromStringReturnsRealForValidRealTypes(String columnTypeName) {
        FireboltColumnDataType result = FireboltColumnDataType.fromString(columnTypeName);
        
        assertEquals(FireboltColumnDataType.REAL, result);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "text",
        "varchar", 
        "boolean",
        "date",
        "unsupported_type",
        "int32",
        "array",
        "array)",
        "(array)",
        "integer_type",
        "int_type"
    })
    void testFromStringThrowsExceptionForInvalidTypes(String columnTypeName) {
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> FireboltColumnDataType.fromString(columnTypeName));
        
        assertEquals("Invalid column type name: " + columnTypeName, exception.getMessage());
    }
    
    @Test
    void testFromStringThrowsExceptionForEmptyString() {
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> FireboltColumnDataType.fromString(""));
        
        assertEquals("Invalid column type name: ", exception.getMessage());
    }

    @Test
    void testFromStringThrowsExceptionForWhitespaceString() {
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> FireboltColumnDataType.fromString("   "));
        
        assertEquals("Invalid column type name:    ", exception.getMessage());
    }

    @Test
    void testEnumValues() {
        FireboltColumnDataType[] values = FireboltColumnDataType.values();
        
        assertEquals(7, values.length);
        assertEquals(FireboltColumnDataType.INTEGER, values[0]);
        assertEquals(FireboltColumnDataType.ARRAY, values[1]);
        assertEquals(FireboltColumnDataType.TIMESTAMP, values[2]);
        assertEquals(FireboltColumnDataType.TIMESTAMPTZ, values[3]);
        assertEquals(FireboltColumnDataType.DECIMAL, values[4]);
        assertEquals(FireboltColumnDataType.BIGINT, values[5]);
        assertEquals(FireboltColumnDataType.REAL, values[6]);
    }

    @Test
    void testEnumValueOf() {
        assertEquals(FireboltColumnDataType.INTEGER, FireboltColumnDataType.valueOf("INTEGER"));
        assertEquals(FireboltColumnDataType.ARRAY, FireboltColumnDataType.valueOf("ARRAY"));
        assertEquals(FireboltColumnDataType.TIMESTAMP, FireboltColumnDataType.valueOf("TIMESTAMP"));
        assertEquals(FireboltColumnDataType.TIMESTAMPTZ, FireboltColumnDataType.valueOf("TIMESTAMPTZ"));
        assertEquals(FireboltColumnDataType.DECIMAL, FireboltColumnDataType.valueOf("DECIMAL"));
        assertEquals(FireboltColumnDataType.BIGINT, FireboltColumnDataType.valueOf("BIGINT"));
        assertEquals(FireboltColumnDataType.REAL, FireboltColumnDataType.valueOf("REAL"));
    }

    @Test
    void testEnumValueOfThrowsExceptionForInvalidName() {
        assertThrows(IllegalArgumentException.class, 
            () -> FireboltColumnDataType.valueOf("INVALID"));
    }
} 