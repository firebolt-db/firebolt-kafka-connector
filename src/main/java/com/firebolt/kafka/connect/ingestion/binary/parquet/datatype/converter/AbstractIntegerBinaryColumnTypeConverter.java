package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;

public abstract class AbstractIntegerBinaryColumnTypeConverter<T extends SchemalessKafkaMessageColumnValue> extends AbstractBinaryColumnTypeConverter<T, Integer> {

    protected Integer toParquetValueInternal(T schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).intValue();
        }

        // check if the long value can "fit" into an int value without data loss
        if (value instanceof Long) {
            Long longValue = (Long) value;
            if (longValue >= Integer.MIN_VALUE && longValue<= Integer.MAX_VALUE) {
                return longValue.intValue();
            } else {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "You are trying to set a long value into an int value column. That would result in data loss.");
            }
        }

        if (value instanceof String) {
            String str = (String) value;
            try {
                int parsed = Integer.parseInt(str.trim());
                return parsed;
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert kafka message attribute to a integer due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

    @Override
    public Class<Integer> getConvertedType() {
        return Integer.class;
    }

}
