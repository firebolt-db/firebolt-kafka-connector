package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts a schemaless kafka message column value to a real (float) that can be written to a parquet data format
 */
@Slf4j
public class SchemalessRealBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue, Float> {

    @Override
    public Float toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof Float) {
            return ((Number) value).floatValue();
        }

        if (value instanceof Double && isExactlyRepresentableAsFloat((Double) value)) {
            return ((Double) value).floatValue();
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

    boolean isExactlyRepresentableAsFloat(double d) {
        // Preserve NaN/infinities handling
        if (Double.isNaN(d)) return true;
        float f = (float) d;
        // Overflow or underflow to infinity -> precision loss
        return Float.isFinite(f);
    }
}


