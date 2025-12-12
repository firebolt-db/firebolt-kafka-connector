package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;


public interface BinaryColumnDataTypeConverter<T extends KafkaMessageColumnValue, R> {

    R toParquetValue(T value, TableSchema.Column tableColumn) throws ColumnConversionFailedException;

    /**
     * Retuns the class of the converted value. E.g if we convert to an integer this would be then Integer.class
     * @return
     */
    Class<R> getConvertedType();
}
