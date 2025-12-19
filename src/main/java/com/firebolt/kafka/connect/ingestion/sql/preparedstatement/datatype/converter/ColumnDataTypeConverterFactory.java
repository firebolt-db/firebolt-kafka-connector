package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;

public interface ColumnDataTypeConverterFactory {

    /**
     * Returns a converter that knows to convert to the target column in firebolt
     * @param fireboltTableColumn
     * @return
     */
    ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn);
}
