package com.firebolt.kafka.connect.ingestion.binary.parquet;


public class BinaryColumnDataTypeFactoryProvider {

    private static BinaryColumnDataTypeFactoryProvider instance;

    private SchemaBinaryColumnDataTypeConverterFactory schemaColumnDataTypeFactory;
    private SchemalessBinaryColumnDataTypeConverterFactory schemalessColumnDataTypeFactory;

    private BinaryColumnDataTypeFactoryProvider() {
        this.schemaColumnDataTypeFactory = new SchemaBinaryColumnDataTypeConverterFactory();
        this.schemalessColumnDataTypeFactory = new SchemalessBinaryColumnDataTypeConverterFactory();
    }

    public static BinaryColumnDataTypeConverterFactory getInstance(boolean hasSchema) {
        if (instance == null) {
            instance = new BinaryColumnDataTypeFactoryProvider();
        }

        return hasSchema ? instance.schemaColumnDataTypeFactory : instance.schemalessColumnDataTypeFactory;
    }
}
