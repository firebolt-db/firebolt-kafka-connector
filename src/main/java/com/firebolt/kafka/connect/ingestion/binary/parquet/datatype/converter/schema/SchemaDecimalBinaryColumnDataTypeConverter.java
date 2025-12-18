package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractDecimalBinaryColumnTypeConverter;
import java.nio.ByteBuffer;

/**
 * Schema-based decimal to Avro bytes via shared abstract implementation.
 */
public class SchemaDecimalBinaryColumnDataTypeConverter extends AbstractDecimalBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    @Override
    public ByteBuffer toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemaKafkaMessageColumnValue, fireboltColumn);
    }
}


