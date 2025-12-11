package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractBigIntBinaryColumnTypeConverter;

/**
 * Schema-based adapter for bigint (long) conversion.
 */
public class SchemaBigIntBinaryBinaryColumnDataTypeConverter extends AbstractBigIntBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public Long toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue,
                               TableSchema.Column tableColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemaKafkaMessageColumnValue, tableColumn);
    }
}

