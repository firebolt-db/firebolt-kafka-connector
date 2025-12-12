package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryBinaryColumnTypeConverter;

/**
 * Converts a schemaless kafka message column value to a bigint (long) that can be written to a parquet data format
 */
public class SchemalessBigIntBinaryColumnDataTypeConverter extends AbstractBinaryBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue, Long> {

    @Override
    public Long toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            String stringValue = (String) value;
            try {
                long parsed = Long.parseLong(stringValue.trim());
                return parsed;
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(
                        fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a bigint due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

    @Override
    public Class<Long> getConvertedType() {
        return Long.class;
    }
}


