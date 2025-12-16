package com.firebolt.kafka.connect.ingestion.binary.parquet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinaryColumnDataTypeFactoryProviderTest {

    @Test
    void returnsSameSchemaFactoryWhenHasSchemaTrue() {
        BinaryColumnDataTypeConverterFactory first = BinaryColumnDataTypeFactoryProvider.getInstance(true);
        BinaryColumnDataTypeConverterFactory second = BinaryColumnDataTypeFactoryProvider.getInstance(true);

        assertTrue(first instanceof SchemaBinaryColumnDataTypeConverterFactory);
        assertSame(first, second, "Expected same schema factory instance across calls");
    }

    @Test
    void returnsSameSchemalessFactoryWhenHasSchemaFalse() {
        BinaryColumnDataTypeConverterFactory first = BinaryColumnDataTypeFactoryProvider.getInstance(false);
        BinaryColumnDataTypeConverterFactory second = BinaryColumnDataTypeFactoryProvider.getInstance(false);

        assertTrue(first instanceof SchemalessBinaryColumnDataTypeConverterFactory);
        assertSame(first, second, "Expected same schemaless factory instance across calls");
    }

    @Test
    void schemaAndSchemalessFactoriesAreDifferentInstances() {
        BinaryColumnDataTypeConverterFactory withSchema = BinaryColumnDataTypeFactoryProvider.getInstance(true);
        BinaryColumnDataTypeConverterFactory withoutSchema = BinaryColumnDataTypeFactoryProvider.getInstance(false);

        assertNotSame(withSchema, withoutSchema, "Schema and schemaless factories must differ");
    }
}

