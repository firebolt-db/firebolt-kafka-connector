package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaArrayBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaBigIntBinaryBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaIntegerBinaryColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemaBinaryColumnDataTypeConverterFactory implements BinaryColumnDataTypeConverterFactory {

    private SchemaIntegerBinaryColumnDataTypeConverter integerDataTypeConverter;
    private SchemaBigIntBinaryBinaryColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemaArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemaBinaryColumnDataTypeConverterFactory() {
        this(new SchemaIntegerBinaryColumnDataTypeConverter(),
             new SchemaBigIntBinaryBinaryColumnDataTypeConverter(),
             new SchemaArrayBinaryColumnDataTypeConverter());
    }

    @VisibleForTesting
    SchemaBinaryColumnDataTypeConverterFactory(SchemaIntegerBinaryColumnDataTypeConverter integerDataTypeConverter,
                                               SchemaBigIntBinaryBinaryColumnDataTypeConverter bigIntDataTypeConverter,
                                               SchemaArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.arrayColumnDataTypeConverter = arrayColumnDataTypeConverter;

        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.INTEGER, integerDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BIGINT, bigIntDataTypeConverter);
    }

    @Override
    public BinaryColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case ARRAY:
                return arrayColumnDataTypeConverter;
            case STRUCT:
                break;
            case GEOGRAPHY:
                break;
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }

}
