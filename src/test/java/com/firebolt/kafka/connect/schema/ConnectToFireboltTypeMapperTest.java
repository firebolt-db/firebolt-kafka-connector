package com.firebolt.kafka.connect.schema;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConnectToFireboltTypeMapperTest {

    // -------------------------------------------------------------------------
    // Primitive types
    // -------------------------------------------------------------------------

    static Stream<Arguments> primitiveTypeMappings() {
        return Stream.of(
            Arguments.of(Schema.INT8_SCHEMA,   "INTEGER"),
            Arguments.of(Schema.INT16_SCHEMA,  "INTEGER"),
            Arguments.of(Schema.INT32_SCHEMA,  "INTEGER"),
            Arguments.of(Schema.INT64_SCHEMA,  "BIGINT"),
            Arguments.of(Schema.FLOAT32_SCHEMA, "REAL"),
            Arguments.of(Schema.FLOAT64_SCHEMA, "DOUBLE PRECISION"),
            Arguments.of(Schema.BOOLEAN_SCHEMA, "BOOLEAN"),
            Arguments.of(Schema.STRING_SCHEMA,  "TEXT"),
            Arguments.of(Schema.BYTES_SCHEMA,   "BYTEA")
        );
    }

    @ParameterizedTest(name = "{0} → {1}")
    @MethodSource("primitiveTypeMappings")
    void primitiveMapsToExpectedFireboltType(Schema schema, String expected) {
        assertEquals(expected, ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    // -------------------------------------------------------------------------
    // Logical types
    // -------------------------------------------------------------------------

    @Test
    void decimalWithScale2MapsToNumeric() {
        Schema schema = Decimal.schema(2);
        assertEquals("NUMERIC(38, 2)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void decimalWithScale5MapsToNumeric() {
        Schema schema = Decimal.schema(5);
        assertEquals("NUMERIC(38, 5)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void decimalWithScale0MapsToNumeric() {
        Schema schema = Decimal.schema(0);
        assertEquals("NUMERIC(38, 0)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void dateMapsToDate() {
        Schema schema = Date.SCHEMA;
        assertEquals("DATE", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void timestampMapsToTimestamp() {
        Schema schema = Timestamp.SCHEMA;
        assertEquals("TIMESTAMP", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    // -------------------------------------------------------------------------
    // ARRAY types
    // -------------------------------------------------------------------------

    @Test
    void arrayOfOptionalStringMapsToArrayTextNull() {
        Schema schema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        assertEquals("ARRAY(TEXT NULL)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void arrayOfRequiredStringMapsToArrayTextNotNull() {
        Schema schema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        assertEquals("ARRAY(TEXT NOT NULL)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void arrayOfOptionalInt32MapsToArrayIntegerNull() {
        Schema schema = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build();
        assertEquals("ARRAY(INTEGER NULL)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void arrayOfRequiredInt64MapsToArrayBigintNotNull() {
        Schema schema = SchemaBuilder.array(Schema.INT64_SCHEMA).build();
        assertEquals("ARRAY(BIGINT NOT NULL)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void arrayOfOptionalBooleanMapsToArrayBooleanNull() {
        Schema schema = SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).build();
        assertEquals("ARRAY(BOOLEAN NULL)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void nestedArrayMapsToNestedArrayType() {
        // ARRAY(ARRAY(TEXT NULL) NOT NULL)
        Schema innerArray = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
        Schema outerArray = SchemaBuilder.array(innerArray).build();
        assertEquals("ARRAY(ARRAY(TEXT NULL) NOT NULL)", ConnectToFireboltTypeMapper.toFireboltType(outerArray));
    }

    @Test
    void arrayOfStructReturnsNull() {
        Schema structSchema = SchemaBuilder.struct()
                .field("x", Schema.INT32_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.array(structSchema).build();
        assertNull(ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    // -------------------------------------------------------------------------
    // Unsupported types return null
    // -------------------------------------------------------------------------

    @Test
    void structReturnsNull() {
        Schema schema = SchemaBuilder.struct()
                .field("a", Schema.STRING_SCHEMA)
                .build();
        assertNull(ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void mapReturnsNull() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        assertNull(ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    // -------------------------------------------------------------------------
    // Logical types take precedence over raw type
    // -------------------------------------------------------------------------

    @Test
    void logicalTypeTakesPrecedenceOverRawBytes() {
        // Decimal has raw type BYTES but should map to NUMERIC
        Schema schema = Decimal.schema(3);
        assertEquals(Schema.Type.BYTES, schema.type());
        assertEquals("NUMERIC(38, 3)", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }

    @Test
    void logicalTypeTakesPrecedenceOverRawInt32() {
        // Date has raw type INT32 but should map to DATE
        Schema schema = Date.SCHEMA;
        assertEquals(Schema.Type.INT32, schema.type());
        assertEquals("DATE", ConnectToFireboltTypeMapper.toFireboltType(schema));
    }
}
