package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

public class ColumnDataTypeFactoryProvider {

    private static ColumnDataTypeFactoryProvider instance;

    private SchemaColumnTypeConverterFactory schemaColumnTypeConverterFactory;
    private SchemalessColumnTypeConverterFactory schemalessColumnTypeConverterFactory;

    private ColumnDataTypeFactoryProvider() {
        this.schemaColumnTypeConverterFactory = new SchemaColumnTypeConverterFactory();
        this.schemalessColumnTypeConverterFactory = new SchemalessColumnTypeConverterFactory();
    }

    public static ColumnDataTypeConverterFactory getInstance(boolean hasSchema) {
        if (instance == null) {
            instance = new ColumnDataTypeFactoryProvider();
        }

        return hasSchema ? instance.schemaColumnTypeConverterFactory : instance.schemalessColumnTypeConverterFactory;
    }
}
