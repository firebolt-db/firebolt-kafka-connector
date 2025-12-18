package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.AbstractRealBinaryColumnTypeConverter;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts a schemaless kafka message column value to a real (float) that can be written to a parquet data format
 */
@Slf4j
public class SchemalessRealBinaryColumnDataTypeConverter extends AbstractRealBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

	@Override
	public Float toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
		return toParquetValueInternal(schemalessKafkaMessageColumnValue, fireboltColumn);
	}
}

