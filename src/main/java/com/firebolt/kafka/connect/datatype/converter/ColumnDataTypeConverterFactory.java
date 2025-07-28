package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.google.common.annotations.VisibleForTesting;

public class ColumnDataTypeConverterFactory {

    private static ColumnDataTypeConverterFactory instance;

    private IntegerDataTypeConverter integerDataTypeConverter ;
    private ArrayDataTypeConverter arrayDataTypeConverter;

    private ColumnDataTypeConverterFactory() {
        // use the static method to create the object
        this(new IntegerDataTypeConverter(), new ArrayDataTypeConverter());
    }

    @VisibleForTesting
    ColumnDataTypeConverterFactory(IntegerDataTypeConverter integerDataTypeConverter, ArrayDataTypeConverter arrayDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.arrayDataTypeConverter = arrayDataTypeConverter;
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
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }
}
