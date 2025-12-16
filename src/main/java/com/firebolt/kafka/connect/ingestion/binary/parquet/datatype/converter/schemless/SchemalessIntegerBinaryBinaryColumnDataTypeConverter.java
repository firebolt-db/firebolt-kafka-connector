package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractIntegerBinaryColumnTypeConverter;

/**
 * Converts a schemaless kafka message column value to an integer that can be written to a parquet data format
 */
public class SchemalessIntegerBinaryBinaryColumnDataTypeConverter extends AbstractIntegerBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public Integer toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemalessKafkaMessageColumnValue, fireboltColumn);
    }

}
