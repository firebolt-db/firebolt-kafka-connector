package com.firebolt.kafka.connect.ingestion.binary.parquet;


public class BinaryColumnDataTypeFactoryProvider {

    private static BinaryColumnDataTypeFactoryProvider instance;

    private SchemalessBinaryColumnDataTypeConverterFactory schemalessColumnDataTypeFactory;

    private BinaryColumnDataTypeFactoryProvider() {
        this.schemalessColumnDataTypeFactory = new SchemalessBinaryColumnDataTypeConverterFactory();
    }

    public static BinaryColumnDataTypeConverterFactory getInstance() {
        if (instance == null) {
            instance = new BinaryColumnDataTypeFactoryProvider();
        }

        // will implement the schema as well, for now assume schemaless
        return instance.schemalessColumnDataTypeFactory;
    }
}
