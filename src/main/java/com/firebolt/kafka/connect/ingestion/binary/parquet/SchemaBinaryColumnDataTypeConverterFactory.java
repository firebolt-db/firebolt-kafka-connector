package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaArrayBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaBigIntBinaryBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaIntegerBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaRealBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaDoubleBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaDecimalBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTimestampBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTimestamptzBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaDateBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTextBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaByteaBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaBooleanBinaryColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemaBinaryColumnDataTypeConverterFactory implements BinaryColumnDataTypeConverterFactory {

    private SchemaIntegerBinaryColumnDataTypeConverter integerDataTypeConverter;
    private SchemaBigIntBinaryBinaryColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemaTimestampBinaryColumnDataTypeConverter timestampDataTypeConverter;
    private SchemaTimestamptzBinaryColumnDataTypeConverter timestamptzDataTypeConverter;
    private SchemaRealBinaryColumnDataTypeConverter realDataTypeConverter;
    private SchemaDoubleBinaryColumnDataTypeConverter doubleDataTypeConverter;
    private SchemaDecimalBinaryColumnDataTypeConverter decimalDataTypeConverter;
    private SchemaDateBinaryColumnDataTypeConverter dateDataTypeConverter;
    private SchemaTextBinaryColumnDataTypeConverter textDataTypeConverter;
    private SchemaByteaBinaryColumnDataTypeConverter byteaDataTypeConverter;
    private SchemaBooleanBinaryColumnDataTypeConverter booleanDataTypeConverter;
    private SchemaArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemaBinaryColumnDataTypeConverterFactory() {
        this(new SchemaIntegerBinaryColumnDataTypeConverter(),
             new SchemaBigIntBinaryBinaryColumnDataTypeConverter(),
             new SchemaTimestampBinaryColumnDataTypeConverter(),
             new SchemaTimestamptzBinaryColumnDataTypeConverter(),
             new SchemaRealBinaryColumnDataTypeConverter(),
             new SchemaDoubleBinaryColumnDataTypeConverter(),
             new SchemaDecimalBinaryColumnDataTypeConverter(),
             new SchemaDateBinaryColumnDataTypeConverter(),
             new SchemaTextBinaryColumnDataTypeConverter(),
             new SchemaByteaBinaryColumnDataTypeConverter(),
             new SchemaBooleanBinaryColumnDataTypeConverter(),
             new SchemaArrayBinaryColumnDataTypeConverter());
    }

    @VisibleForTesting
    SchemaBinaryColumnDataTypeConverterFactory(SchemaIntegerBinaryColumnDataTypeConverter integerDataTypeConverter,
                                               SchemaBigIntBinaryBinaryColumnDataTypeConverter bigIntDataTypeConverter,
                                               SchemaTimestampBinaryColumnDataTypeConverter timestampDataTypeConverter,
                                               SchemaTimestamptzBinaryColumnDataTypeConverter timestamptzDataTypeConverter,
                                               SchemaRealBinaryColumnDataTypeConverter realDataTypeConverter,
                                               SchemaDoubleBinaryColumnDataTypeConverter doubleDataTypeConverter,
                                               SchemaDecimalBinaryColumnDataTypeConverter decimalDataTypeConverter,
                                               SchemaDateBinaryColumnDataTypeConverter dateDataTypeConverter,
                                               SchemaTextBinaryColumnDataTypeConverter textDataTypeConverter,
                                               SchemaByteaBinaryColumnDataTypeConverter byteaDataTypeConverter,
                                               SchemaBooleanBinaryColumnDataTypeConverter booleanDataTypeConverter,
                                               SchemaArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.doubleDataTypeConverter = doubleDataTypeConverter;
        this.decimalDataTypeConverter = decimalDataTypeConverter;
        this.dateDataTypeConverter = dateDataTypeConverter;
        this.textDataTypeConverter = textDataTypeConverter;
        this.byteaDataTypeConverter = byteaDataTypeConverter;
        this.booleanDataTypeConverter = booleanDataTypeConverter;
        this.arrayColumnDataTypeConverter = arrayColumnDataTypeConverter;

        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.INTEGER, integerDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BIGINT, bigIntDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TIMESTAMP, timestampDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TIMESTAMPTZ, timestamptzDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.REAL, realDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.DOUBLE, doubleDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.DATE, dateDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TEXT, textDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BYTEA, byteaDataTypeConverter);
        this.arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BOOLEAN, booleanDataTypeConverter);
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
            case DOUBLE:
                return doubleDataTypeConverter;
            case DECIMAL:
                return decimalDataTypeConverter;
            case DATE:
                return dateDataTypeConverter;
            case TEXT:
                return textDataTypeConverter;
            case BYTEA:
                return byteaDataTypeConverter;
            case BOOLEAN:
                return booleanDataTypeConverter;
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
