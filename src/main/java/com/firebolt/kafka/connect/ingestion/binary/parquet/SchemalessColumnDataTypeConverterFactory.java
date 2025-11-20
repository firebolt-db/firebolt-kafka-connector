package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemalessColumnDataTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter;

    public SchemalessColumnDataTypeConverterFactory() {
        this(
                new SchemalessIntegerColumnDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemalessColumnDataTypeConverterFactory(SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
    }

    @Override
    public ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }
}
