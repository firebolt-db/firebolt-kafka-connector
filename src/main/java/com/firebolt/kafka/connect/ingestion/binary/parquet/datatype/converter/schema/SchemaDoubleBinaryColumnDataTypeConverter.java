package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractDoubleBinaryColumnTypeConverter;

/**
 * Schema-based adapter converting values to parquet/avro double for double precision type.
 */
public class SchemaDoubleBinaryColumnDataTypeConverter extends AbstractDoubleBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public Double toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemaKafkaMessageColumnValue, fireboltColumn);
    }
}


