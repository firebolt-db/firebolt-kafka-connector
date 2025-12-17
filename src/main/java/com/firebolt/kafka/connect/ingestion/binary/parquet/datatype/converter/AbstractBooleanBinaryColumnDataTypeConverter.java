package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;

/**
 * Shared boolean conversion for binary parquet ingestion.
 * Accepts Boolean, String (true/false/t/f/1/0), Number (0/1).
 */
public abstract class AbstractBooleanBinaryColumnDataTypeConverter<T extends SchemalessKafkaMessageColumnValue> extends AbstractBinaryColumnTypeConverter<T, Boolean> {

    @Override
    public Boolean toParquetValue(T kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        if (value instanceof String) {
            String s = ((String) value).trim().toLowerCase();
            if ("true".equals(s) || "t".equals(s) || "1".equals(s)) {
                return Boolean.TRUE;
            }
            if ("false".equals(s) || "f".equals(s) || "0".equals(s)) {
                return Boolean.FALSE;
            }
            throw new ColumnConversionFailedException(
                    fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "Cannot convert kafka message attribute to a boolean due to incompatible string: " + value);
        }

        if (value instanceof Number) {
            long n = ((Number) value).longValue();
            if (n == 0L) return Boolean.FALSE;
            if (n == 1L) return Boolean.TRUE;
            throw new ColumnConversionFailedException(
                    fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "Cannot convert kafka message numeric value to a boolean unless 0 or 1");
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

    @Override
    public Class<Boolean> getConvertedType() {
        return Boolean.class;
    }
}


