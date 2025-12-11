package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractIntegerBinaryColumnTypeConverter;

/**
 * Schema-based adapter for integer conversion that reuses the schemaless converter logic.
 */
public class SchemaIntegerBinaryColumnDataTypeConverter extends AbstractIntegerBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public Integer toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue,
                                  TableSchema.Column tableColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemaKafkaMessageColumnValue, tableColumn);
    }

}

