package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessArrayBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBigIntBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestampBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestamptzBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessRealBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDecimalBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDoubleBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDateBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTextBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBooleanBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessByteaBinaryColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemalessBinaryColumnDataTypeConverterFactory implements BinaryColumnDataTypeConverterFactory {

    private SchemalessIntegerBinaryColumnDataTypeConverter integerDataTypeConverter;
    private SchemalessBigIntBinaryColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemalessTimestampBinaryColumnDataTypeConverter timestampDataTypeConverter;
    private SchemalessTimestamptzBinaryColumnDataTypeConverter timestamptzDataTypeConverter;
    private SchemalessRealBinaryColumnDataTypeConverter realDataTypeConverter;
    private SchemalessDecimalBinaryColumnDataTypeConverter decimalDataTypeConverter;
    private SchemalessDoubleBinaryColumnDataTypeConverter doubleDataTypeConverter;
    private SchemalessDateBinaryColumnDataTypeConverter dateDataTypeConverter;
    private SchemalessTextBinaryColumnDataTypeConverter textDataTypeConverter;
    private SchemalessBooleanBinaryColumnDataTypeConverter booleanDataTypeConverter;
    private SchemalessByteaBinaryColumnDataTypeConverter byteaDataTypeConverter;
    private SchemalessArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemalessBinaryColumnDataTypeConverterFactory() {
        this(
                new SchemalessIntegerBinaryColumnDataTypeConverter(),
                new SchemalessBigIntBinaryColumnDataTypeConverter(),
                new SchemalessTimestampBinaryColumnDataTypeConverter(),
                new SchemalessTimestamptzBinaryColumnDataTypeConverter(),
                new SchemalessRealBinaryColumnDataTypeConverter(),
                new SchemalessDecimalBinaryColumnDataTypeConverter(),
                new SchemalessDoubleBinaryColumnDataTypeConverter(),
                new SchemalessDateBinaryColumnDataTypeConverter(),
                new SchemalessTextBinaryColumnDataTypeConverter(),
                new SchemalessBooleanBinaryColumnDataTypeConverter(),
                new SchemalessByteaBinaryColumnDataTypeConverter(),
                new SchemalessArrayBinaryColumnDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemalessBinaryColumnDataTypeConverterFactory(SchemalessIntegerBinaryColumnDataTypeConverter integerDataTypeConverter,
                                                   SchemalessBigIntBinaryColumnDataTypeConverter bigIntDataTypeConverter,
                                                   SchemalessTimestampBinaryColumnDataTypeConverter timestampDataTypeConverter,
                                                   SchemalessTimestamptzBinaryColumnDataTypeConverter timestamptzDataTypeConverter,
                                                   SchemalessRealBinaryColumnDataTypeConverter realDataTypeConverter,
                                                   SchemalessDecimalBinaryColumnDataTypeConverter decimalDataTypeConverter,
                                                   SchemalessDoubleBinaryColumnDataTypeConverter doubleDataTypeConverter,
                                                   SchemalessDateBinaryColumnDataTypeConverter dateDataTypeConverter,
                                                   SchemalessTextBinaryColumnDataTypeConverter textDataTypeConverter,
                                                   SchemalessBooleanBinaryColumnDataTypeConverter booleanDataTypeConverter,
                                                   SchemalessByteaBinaryColumnDataTypeConverter byteaDataTypeConverter,
                                                   SchemalessArrayBinaryColumnDataTypeConverter arrayColumnDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.decimalDataTypeConverter = decimalDataTypeConverter;
        this.doubleDataTypeConverter = doubleDataTypeConverter;
        this.dateDataTypeConverter = dateDataTypeConverter;
        this.textDataTypeConverter = textDataTypeConverter;
        this.booleanDataTypeConverter = booleanDataTypeConverter;
        this.byteaDataTypeConverter = byteaDataTypeConverter;
        this.arrayColumnDataTypeConverter = arrayColumnDataTypeConverter;

        // need to populate the array column data type with the values that it can convert
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.INTEGER, integerDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BIGINT, bigIntDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.REAL, realDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.DOUBLE, doubleDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.DATE, dateDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TEXT, textDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BOOLEAN, booleanDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TIMESTAMP, timestampDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.TIMESTAMPTZ, timestamptzDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(FireboltColumnDataType.BYTEA, byteaDataTypeConverter);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public BinaryColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case TIMESTAMP:
                return timestampDataTypeConverter;
            case TIMESTAMPTZ:
                return timestamptzDataTypeConverter;
            case REAL:
                return realDataTypeConverter;
            case DECIMAL:
                return decimalDataTypeConverter;
            case DOUBLE:
                return doubleDataTypeConverter;
            case DATE:
                return dateDataTypeConverter;
            case TEXT:
                return textDataTypeConverter;
            case BOOLEAN:
                return booleanDataTypeConverter;
            case BYTEA:
                return byteaDataTypeConverter;
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
