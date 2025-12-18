package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaArrayBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaBigIntBinaryBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaIntegerBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaRealBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTimestampBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTimestamptzBinaryColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemaBinaryColumnDataTypeConverterFactory implements BinaryColumnDataTypeConverterFactory {

    private SchemaIntegerBinaryColumnDataTypeConverter integerDataTypeConverter;
    private SchemaBigIntBinaryBinaryColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemaTimestampBinaryColumnDataTypeConverter timestampDataTypeConverter;
    private SchemaTimestamptzBinaryColumnDataTypeConverter timestamptzDataTypeConverter;
    private SchemaRealBinaryColumnDataTypeConverter realDataTypeConverter;
    private SchemaArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemaBinaryColumnDataTypeConverterFactory() {
        this(new SchemaIntegerBinaryColumnDataTypeConverter(),
             new SchemaBigIntBinaryBinaryColumnDataTypeConverter(),
             new SchemaTimestampBinaryColumnDataTypeConverter(),
             new SchemaTimestamptzBinaryColumnDataTypeConverter(),
             new SchemaRealBinaryColumnDataTypeConverter(),
             new SchemaArrayBinaryColumnDataTypeConverter());
    }

    @VisibleForTesting
    SchemaBinaryColumnDataTypeConverterFactory(SchemaIntegerBinaryColumnDataTypeConverter integerDataTypeConverter,
                                               SchemaBigIntBinaryBinaryColumnDataTypeConverter bigIntDataTypeConverter,
                                               SchemaTimestampBinaryColumnDataTypeConverter timestampDataTypeConverter,
                                               SchemaTimestamptzBinaryColumnDataTypeConverter timestamptzDataTypeConverter,
                                               SchemaRealBinaryColumnDataTypeConverter realDataTypeConverter,
                                               SchemaArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.arrayColumnDataTypeConverter = arrayColumnDataTypeConverter;

        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.INTEGER, integerDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BIGINT, bigIntDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TIMESTAMP, timestampDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TIMESTAMPTZ, timestamptzDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.REAL, realDataTypeConverter);
    }

    @Override
    public BinaryColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case REAL:
                return realDataTypeConverter;
            case TIMESTAMP:
                return timestampDataTypeConverter;
            case TIMESTAMPTZ:
                return timestamptzDataTypeConverter;
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
