package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Converts a schemaless kafka message column value to Avro decimal logical type (BYTES) using precision/scale if available.
 */
public class SchemalessDecimalColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, ByteBuffer> {

    @Override
    public ByteBuffer toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        BigDecimal decimal = toBigDecimal(value, fireboltColumn);
        int precision = fireboltColumn.getPrecision();
        int scale = fireboltColumn.getScale();

        LogicalTypes.Decimal lt = LogicalTypes.decimal(precision, scale);
        Schema schema = lt.addToSchema(Schema.create(Schema.Type.BYTES));
        Conversions.DecimalConversion conv = new Conversions.DecimalConversion();
        try {
            return conv.toBytes(decimal, schema, lt);
        } catch (Exception e) {
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "Cannot encode decimal to Avro bytes: " + e.getMessage());
        }
    }

    private BigDecimal toBigDecimal(Object value, TableSchema.Column fireboltColumn) {
        BigDecimal decimal;
        if (value instanceof String) {
            String s = (String) value;
            try {
                decimal = new BigDecimal(s.trim());
            } catch (Exception ex) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a decimal value in firebolt");
            }
        } else if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            decimal = BigDecimal.valueOf(((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            decimal = BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "Cannot convert kafka message attribute to a decimal value in firebolt");
        }

        // Align to target scale with HALF_UP rounding to avoid Avro ArithmeticException
        int targetScale = fireboltColumn.getScale();
        BigDecimal scaled = decimal.setScale(targetScale, RoundingMode.HALF_UP);

        // Validate precision does not exceed target precision after scaling
        int targetPrecision = fireboltColumn.getPrecision();
        int digits = scaled.precision();
        if (targetPrecision > 0 && digits > targetPrecision) {
            throw new ColumnConversionFailedException(
                    fireboltColumn.getName(),
                    fireboltColumn.getDataType(),
                    "Decimal value " + scaled + " exceeds precision " + targetPrecision + " for column");
        }
        return scaled;
    }

    @Override
    public Class<ByteBuffer> getConvertedType() {
        return ByteBuffer.class;
    }
}


