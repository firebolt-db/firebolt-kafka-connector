package com.firebolt.kafka.connect.datatype.converter;

public class ColumnDataTypeColumnFactoryProvider {

    private static ColumnDataTypeColumnFactoryProvider instance;

    private SchemaColumnTypeConverterFactory schemaColumnTypeConverterFactory;
    private SchemalessColumnTypeConverterFactory schemalessColumnTypeConverterFactory;

    private ColumnDataTypeColumnFactoryProvider() {
        this.schemaColumnTypeConverterFactory = new SchemaColumnTypeConverterFactory();
        this.schemalessColumnTypeConverterFactory = new SchemalessColumnTypeConverterFactory();
    }

    public static ColumnDataTypeConverterFactory getInstance(boolean isSchemaless) {
        if (instance == null) {
            instance = new ColumnDataTypeColumnFactoryProvider();
        }

        return isSchemaless ? instance.schemalessColumnTypeConverterFactory : instance.schemaColumnTypeConverterFactory;
    }
}
