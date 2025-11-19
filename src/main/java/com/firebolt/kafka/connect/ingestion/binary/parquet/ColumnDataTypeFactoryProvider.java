package com.firebolt.kafka.connect.ingestion.binary.parquet;


public class ColumnDataTypeFactoryProvider {

    private static ColumnDataTypeFactoryProvider instance;

    private SchemalessColumnDataTypeConverterFactory schemalessColumnDataTypeFactory;

    private ColumnDataTypeFactoryProvider() {
        this.schemalessColumnDataTypeFactory = new SchemalessColumnDataTypeConverterFactory();
    }

    public static ColumnDataTypeConverterFactory getInstance() {
        if (instance == null) {
            instance = new ColumnDataTypeFactoryProvider();
        }

        // will implement the schema as well, for now assume schemaless
        return instance.schemalessColumnDataTypeFactory;
    }
}
