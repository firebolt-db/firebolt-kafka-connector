package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractDoubleBinaryColumnTypeConverter;

/**
 * Converts a schemaless kafka message column value to a double precision (Double) for parquet/avro
 */
public class SchemalessDoubleBinaryColumnDataTypeConverter extends AbstractDoubleBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public Double toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemalessKafkaMessageColumnValue, fireboltColumn);
    }
}


