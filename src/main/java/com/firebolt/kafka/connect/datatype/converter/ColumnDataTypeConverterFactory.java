package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.google.common.annotations.VisibleForTesting;

public interface ColumnDataTypeConverterFactory {

    /**
     * Returns a converter that knows to convert to the target column in firebolt
     * @param fireboltTableColumn
     * @return
     */
    ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn);
}
