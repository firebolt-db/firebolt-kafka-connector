package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessArrayColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBigIntColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemalessColumnDataTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter;
    private SchemalessBigIntColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemalessArrayColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemalessColumnDataTypeConverterFactory() {
        this(
                new SchemalessIntegerColumnDataTypeConverter(),
                new SchemalessBigIntColumnDataTypeConverter(),
                new SchemalessArrayColumnDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemalessColumnDataTypeConverterFactory(SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter,
                                             SchemalessBigIntColumnDataTypeConverter bigIntDataTypeConverter,
                                             SchemalessArrayColumnDataTypeConverter arrayColumnDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.arrayColumnDataTypeConverter = arrayColumnDataTypeConverter;

        // need to populate the array column data type with the values that it can convert
        arrayColumnDataTypeConverter.addConverter(Integer.class, integerDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(Long.class, bigIntDataTypeConverter);
    }

    @Override
    public ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case ARRAY:
                return arrayColumnDataTypeConverter;
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }
}
