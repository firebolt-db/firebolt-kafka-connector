package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessArrayColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBigIntColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestampColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestamptzColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessRealColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDecimalColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDoubleColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDateColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTextColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBooleanColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessByteaColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemalessColumnDataTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter;
    private SchemalessBigIntColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemalessTimestampColumnDataTypeConverter timestampDataTypeConverter;
    private SchemalessTimestamptzColumnDataTypeConverter timestamptzDataTypeConverter;
    private SchemalessRealColumnDataTypeConverter realDataTypeConverter;
    private SchemalessDecimalColumnDataTypeConverter decimalDataTypeConverter;
    private SchemalessDoubleColumnDataTypeConverter doubleDataTypeConverter;
    private SchemalessDateColumnDataTypeConverter dateDataTypeConverter;
    private SchemalessTextColumnDataTypeConverter textDataTypeConverter;
    private SchemalessBooleanColumnDataTypeConverter booleanDataTypeConverter;
    private SchemalessByteaColumnDataTypeConverter byteaDataTypeConverter;
    private SchemalessArrayColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemalessColumnDataTypeConverterFactory() {
        this(
                new SchemalessIntegerColumnDataTypeConverter(),
                new SchemalessBigIntColumnDataTypeConverter(),
                new SchemalessTimestampColumnDataTypeConverter(),
                new SchemalessTimestamptzColumnDataTypeConverter(),
                new SchemalessRealColumnDataTypeConverter(),
                new SchemalessDecimalColumnDataTypeConverter(),
                new SchemalessDoubleColumnDataTypeConverter(),
                new SchemalessDateColumnDataTypeConverter(),
                new SchemalessTextColumnDataTypeConverter(),
                new SchemalessBooleanColumnDataTypeConverter(),
                new SchemalessByteaColumnDataTypeConverter(),
                new SchemalessArrayColumnDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemalessColumnDataTypeConverterFactory(SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter,
                                             SchemalessBigIntColumnDataTypeConverter bigIntDataTypeConverter,
                                             SchemalessTimestampColumnDataTypeConverter timestampDataTypeConverter,
                                             SchemalessTimestamptzColumnDataTypeConverter timestamptzDataTypeConverter,
                                             SchemalessRealColumnDataTypeConverter realDataTypeConverter,
                                             SchemalessDecimalColumnDataTypeConverter decimalDataTypeConverter,
                                             SchemalessDoubleColumnDataTypeConverter doubleDataTypeConverter,
                                             SchemalessDateColumnDataTypeConverter dateDataTypeConverter,
                                             SchemalessTextColumnDataTypeConverter textDataTypeConverter,
                                             SchemalessBooleanColumnDataTypeConverter booleanDataTypeConverter,
                                             SchemalessByteaColumnDataTypeConverter byteaDataTypeConverter,
                                             SchemalessArrayColumnDataTypeConverter arrayColumnDataTypeConverter) {
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
    public ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
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
