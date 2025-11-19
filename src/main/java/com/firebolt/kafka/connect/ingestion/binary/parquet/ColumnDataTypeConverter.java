package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;


public interface ColumnDataTypeConverter<T extends KafkaMessageColumnValue, R> {

    R toParquetValue(T value, TableSchema.Column tableColumn) throws ColumnConversionFailedException;

}
