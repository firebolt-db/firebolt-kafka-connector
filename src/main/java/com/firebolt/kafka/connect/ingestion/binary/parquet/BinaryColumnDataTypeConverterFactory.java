package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;

public interface BinaryColumnDataTypeConverterFactory {

    /**
     * Returns a converter that knows to convert to the target column in firebolt
     * @param fireboltTableColumn
     * @return
     */
    BinaryColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn);
}
