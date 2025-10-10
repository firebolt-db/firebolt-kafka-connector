package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaArrayDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaBigIntDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaBooleanDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaByteaDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaDateDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaDecimalDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaDoubleDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaIntegerDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaRealDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTextDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTimestampDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.schema.SchemaTimestamptzDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemaColumnTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemaIntegerDataTypeConverter integerDataTypeConverter ;
    private SchemaArrayDataTypeConverter arrayDataTypeConverter;
    private SchemaTimestampDataTypeConverter timestampDataTypeConverter;
    private SchemaTimestamptzDataTypeConverter timestamptzDataTypeConverter;
    private SchemaDateDataTypeConverter dateDataTypeConverter;
    private SchemaDecimalDataTypeConverter decimalDataTypeConverter;
    private SchemaBigIntDataTypeConverter bigIntDataTypeConverter;
    private SchemaRealDataTypeConverter realDataTypeConverter;
    private SchemaDoubleDataTypeConverter doubleDataTypeConverter;
    private SchemaTextDataTypeConverter textDataTypeConverter;
    private SchemaByteaDataTypeConverter byteaDataTypeConverter;
    private SchemaBooleanDataTypeConverter booleanDataTypeConverter;

    public SchemaColumnTypeConverterFactory() {
        this(
                new SchemaIntegerDataTypeConverter(),
                new SchemaArrayDataTypeConverter(),
                new SchemaTimestampDataTypeConverter(),
                new SchemaTimestamptzDataTypeConverter(),
                new SchemaDecimalDataTypeConverter(),
                new SchemaBigIntDataTypeConverter(),
                new SchemaRealDataTypeConverter(),
                new SchemaDoubleDataTypeConverter(),
                new SchemaTextDataTypeConverter(),
                new SchemaDateDataTypeConverter(),
                new SchemaByteaDataTypeConverter(),
                new SchemaBooleanDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemaColumnTypeConverterFactory(SchemaIntegerDataTypeConverter integerDataTypeConverter,
                                   SchemaArrayDataTypeConverter arrayDataTypeConverter,
                                   SchemaTimestampDataTypeConverter timestampDataTypeConverter,
                                   SchemaTimestamptzDataTypeConverter timestamptzDataTypeConverter,
                                   SchemaDecimalDataTypeConverter decimalDataTypeConverter,
                                   SchemaBigIntDataTypeConverter bigIntDataTypeConverter,
                                   SchemaRealDataTypeConverter realDataTypeConverter,
                                   SchemaDoubleDataTypeConverter doubleDataTypeConverter,
                                   SchemaTextDataTypeConverter textDataTypeConverter,
                                   SchemaDateDataTypeConverter dateDataTypeConverter,
                                   SchemaByteaDataTypeConverter byteaDataTypeConverter,
                                   SchemaBooleanDataTypeConverter booleanDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.arrayDataTypeConverter = arrayDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.decimalDataTypeConverter = decimalDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.doubleDataTypeConverter = doubleDataTypeConverter;
        this.textDataTypeConverter = textDataTypeConverter;
        this.dateDataTypeConverter = dateDataTypeConverter;
        this.byteaDataTypeConverter = byteaDataTypeConverter;
        this.booleanDataTypeConverter = booleanDataTypeConverter;
    }

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
