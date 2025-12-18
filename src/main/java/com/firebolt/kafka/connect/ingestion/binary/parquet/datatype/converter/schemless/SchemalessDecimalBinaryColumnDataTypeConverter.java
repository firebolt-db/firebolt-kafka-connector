package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractDecimalBinaryColumnTypeConverter;
import java.nio.ByteBuffer;

/**
 * Schemaless decimal to Avro bytes via shared abstract implementation.
 */
public class SchemalessDecimalBinaryColumnDataTypeConverter extends AbstractDecimalBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public ByteBuffer toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        return toParquetValueInternal(schemalessKafkaMessageColumnValue, fireboltColumn);
    }
}

