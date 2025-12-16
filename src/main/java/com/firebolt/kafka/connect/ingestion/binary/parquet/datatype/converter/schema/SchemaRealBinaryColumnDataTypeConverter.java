package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractRealBinaryColumnTypeConverter;

/**
 * Schema-based adapter converting values to parquet/avro float for real type.
 */
public class SchemaRealBinaryColumnDataTypeConverter extends AbstractRealBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue> {

	@Override
	public Float toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
		return toParquetValueInternal(schemaKafkaMessageColumnValue, fireboltColumn);
	}
}


