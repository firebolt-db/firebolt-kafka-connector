package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractJsonBinaryColumnDataTypeConverter;

/**
 * Converts a schemaless kafka message column value to a JSON string for parquet/avro.
 */
public class SchemalessJsonBinaryColumnDataTypeConverter extends AbstractJsonBinaryColumnDataTypeConverter<SchemalessKafkaMessageColumnValue> {
}
