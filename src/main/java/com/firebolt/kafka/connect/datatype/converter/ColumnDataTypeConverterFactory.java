package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.google.common.annotations.VisibleForTesting;

public class ColumnDataTypeConverterFactory {

    private static ColumnDataTypeConverterFactory instance;

    private IntegerDataTypeConverter integerDataTypeConverter ;
    private ArrayDataTypeConverter arrayDataTypeConverter;
    private TimestampDataTypeConverter timestampDataTypeConverter;
    private TimestamptzDataTypeConverter timestamptzDataTypeConverter;
    private DateDataTypeConverter dateDataTypeConverter;
    private DecimalDataTypeConverter decimalDataTypeConverter;
    private BigIntDataTypeConverter bigIntDataTypeConverter;
    private RealDataTypeConverter realDataTypeConverter;
    private DoubleDataTypeConverter doubleDataTypeConverter;
    private TextDataTypeConverter textDataTypeConverter;
    private ByteaDataTypeConverter byteaDataTypeConverter;
    private BooleanDataTypeConverter booleanDataTypeConverter;

    private ColumnDataTypeConverterFactory() {
        // use the static method to create the object
        this(new IntegerDataTypeConverter(), new ArrayDataTypeConverter(), new TimestampDataTypeConverter(),
             new TimestamptzDataTypeConverter(), new DecimalDataTypeConverter(), new BigIntDataTypeConverter(),
             new RealDataTypeConverter(), new DoubleDataTypeConverter(), new TextDataTypeConverter(), new DateDataTypeConverter(),
                new ByteaDataTypeConverter(), new BooleanDataTypeConverter());
    }

    @VisibleForTesting
    ColumnDataTypeConverterFactory(IntegerDataTypeConverter integerDataTypeConverter,
                                   ArrayDataTypeConverter arrayDataTypeConverter,
                                   TimestampDataTypeConverter timestampDataTypeConverter,
                                   TimestamptzDataTypeConverter timestamptzDataTypeConverter,
                                   DecimalDataTypeConverter decimalDataTypeConverter,
                                   BigIntDataTypeConverter bigIntDataTypeConverter,
                                   RealDataTypeConverter realDataTypeConverter,
                                   DoubleDataTypeConverter doubleDataTypeConverter,
                                   TextDataTypeConverter textDataTypeConverter,
                                   DateDataTypeConverter dateDataTypeConverter,
                                   ByteaDataTypeConverter byteaDataTypeConverter,
                                   BooleanDataTypeConverter booleanDataTypeConverter) {
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

    public static ColumnDataTypeConverterFactory getInstance() {
        if (instance == null) {
            instance = new ColumnDataTypeConverterFactory();
        }

        return instance;
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
