package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;

public abstract class AbstractRealBinaryColumnTypeConverter<T extends SchemalessKafkaMessageColumnValue> extends AbstractBinaryColumnTypeConverter<T, Float> {

	protected Float toParquetValueInternal(T kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
		Object value = kafkaMessageColumnValue.getValue();

		if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof Float) {
			return ((Number) value).floatValue();
		}

		if (value instanceof Double) {
			Double d = (Double) value;
			Float f = d.floatValue();
			if (Float.isFinite(f)) {
				return f;
			}
			throw new ColumnConversionFailedException(
					fireboltColumn.getName(), fireboltColumn.getDataType(),
					"Double value cannot be safely converted to real (float)");
		}

		if (value instanceof String) {
			String s = ((String) value).trim();
			try {
				return Float.parseFloat(s);
			} catch (NumberFormatException e) {
				throw new ColumnConversionFailedException(
						fireboltColumn.getName(), fireboltColumn.getDataType(),
						"Cannot convert kafka message attribute to a real due to NumberFormatException: " + e.getMessage());
			}
		}

		throw aColumnConversionFailedException(fireboltColumn, value);
	}

	@Override
	public Class<Float> getConvertedType() {
		return Float.class;
	}
}


