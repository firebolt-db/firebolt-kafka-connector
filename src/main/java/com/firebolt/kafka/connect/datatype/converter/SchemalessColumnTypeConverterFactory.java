package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaBigIntDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaBooleanDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaDoubleDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTextDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessArrayDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessByteaDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessDateDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessDecimalDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessIntegerDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessRealDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessTimestampDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schemaless.SchemalessTimestamptzDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

/**
 * Should be created by the ColumnDataTypeConverterFactory, thus package protected
 */
class SchemalessColumnTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemalessIntegerDataTypeConverter integerDataTypeConverter;
    private SchemalessArrayDataTypeConverter arrayDataTypeConverter;
    private SchemalessTimestampDataTypeConverter timestampDataTypeConverter;
    private SchemalessTimestamptzDataTypeConverter timestamptzDataTypeConverter;
    private SchemalessDateDataTypeConverter dateDataTypeConverter;
    private SchemalessDecimalDataTypeConverter decimalDataTypeConverter;
    private SchemaBigIntDataTypeConverter bigIntDataTypeConverter;
    private SchemalessRealDataTypeConverter realDataTypeConverter;
    private SchemaDoubleDataTypeConverter doubleDataTypeConverter;
    private SchemaTextDataTypeConverter textDataTypeConverter;
    private SchemalessByteaDataTypeConverter byteaDataTypeConverter;
    private SchemaBooleanDataTypeConverter booleanDataTypeConverter;

    public SchemalessColumnTypeConverterFactory() {
        this(
                new SchemalessIntegerDataTypeConverter(),
                new SchemalessArrayDataTypeConverter(),
                new SchemalessTimestampDataTypeConverter(),
                new SchemalessTimestamptzDataTypeConverter(),
                new SchemalessDateDataTypeConverter(),
                new SchemalessDecimalDataTypeConverter(),
                new SchemaBigIntDataTypeConverter(),
                new SchemalessRealDataTypeConverter(),
                new SchemaDoubleDataTypeConverter(),
                new SchemaTextDataTypeConverter(),
                new SchemalessByteaDataTypeConverter(),
                new SchemaBooleanDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemalessColumnTypeConverterFactory(SchemalessIntegerDataTypeConverter integerDataTypeConverter,
                                         SchemalessArrayDataTypeConverter arrayDataTypeConverter,
                                         SchemalessTimestampDataTypeConverter timestampDataTypeConverter,
                                         SchemalessTimestamptzDataTypeConverter timestamptzDataTypeConverter,
                                         SchemalessDateDataTypeConverter dateDataTypeConverter,
                                         SchemalessDecimalDataTypeConverter decimalDataTypeConverter,
                                         SchemaBigIntDataTypeConverter bigIntDataTypeConverter,
                                         SchemalessRealDataTypeConverter realDataTypeConverter,
                                         SchemaDoubleDataTypeConverter doubleDataTypeConverter,
                                         SchemaTextDataTypeConverter textDataTypeConverter,
                                         SchemalessByteaDataTypeConverter byteaDataTypeConverter,
                                         SchemaBooleanDataTypeConverter booleanDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.arrayDataTypeConverter = arrayDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.dateDataTypeConverter = dateDataTypeConverter;
        this.decimalDataTypeConverter = decimalDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.doubleDataTypeConverter = doubleDataTypeConverter;
        this.textDataTypeConverter = textDataTypeConverter;
        this.byteaDataTypeConverter = byteaDataTypeConverter;
        this.booleanDataTypeConverter = booleanDataTypeConverter;
    }

    @Override
    public ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case ARRAY:
                return arrayDataTypeConverter;
            case TIMESTAMP:
                return timestampDataTypeConverter;
            case TIMESTAMPTZ:
                return timestamptzDataTypeConverter;
            case DATE:
                return dateDataTypeConverter;
            case DECIMAL:
                return decimalDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case REAL:
                return realDataTypeConverter;
            case DOUBLE:
                return doubleDataTypeConverter;
            case TEXT:
                return textDataTypeConverter;
            case BYTEA:
                return byteaDataTypeConverter;
            case BOOLEAN:
                return booleanDataTypeConverter;
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }
}
