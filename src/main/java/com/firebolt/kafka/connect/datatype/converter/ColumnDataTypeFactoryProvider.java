package com.firebolt.kafka.connect.datatype.converter;

public class ColumnDataTypeFactoryProvider {

    private static ColumnDataTypeFactoryProvider instance;

    private SchemaColumnTypeConverterFactory schemaColumnTypeConverterFactory;
    private SchemalessColumnTypeConverterFactory schemalessColumnTypeConverterFactory;

    private ColumnDataTypeFactoryProvider() {
        this.schemaColumnTypeConverterFactory = new SchemaColumnTypeConverterFactory();
        this.schemalessColumnTypeConverterFactory = new SchemalessColumnTypeConverterFactory();
    }

    public static ColumnDataTypeConverterFactory getInstance(boolean isSchemaless) {
        if (instance == null) {
            instance = new ColumnDataTypeFactoryProvider();
        }

        return isSchemaless ? instance.schemalessColumnTypeConverterFactory : instance.schemaColumnTypeConverterFactory;
    }
}
