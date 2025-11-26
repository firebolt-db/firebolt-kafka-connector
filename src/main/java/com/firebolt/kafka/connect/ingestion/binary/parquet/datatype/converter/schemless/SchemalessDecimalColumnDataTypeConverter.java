package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Converts a schemaless kafka message column value to Avro decimal logical type (BYTES) using precision/scale if available.
 */
public class SchemalessDecimalColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, ByteBuffer> {

    private static final int DEFAULT_PRECISION = 38;
    private static final int DEFAULT_SCALE = 9;
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(?i)decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("(?i)numeric\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

    @Override
    public ByteBuffer toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        BigDecimal decimal = toBigDecimal(value, fireboltColumn);
        int[] ps = parsePrecisionScale(fireboltColumn.getDataType());
        int precision = ps[0];
        int scale = ps[1];

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

    private static BigDecimal toBigDecimal(Object value, TableSchema.Column fireboltColumn) {
        if (value instanceof String) {
            String s = (String) value;
            try {
                return new BigDecimal(s.trim());
            } catch (Exception ex) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Cannot convert kafka message attribute to a decimal value in firebolt");
            }
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                "Cannot convert kafka message attribute to a decimal value in firebolt");
    }

    private static int[] parsePrecisionScale(String type) {
        if (type == null) return new int[]{DEFAULT_PRECISION, DEFAULT_SCALE};
        Matcher m = DECIMAL_PATTERN.matcher(type);
        if (m.find()) {
            return new int[]{Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))};
        }
        Matcher n = NUMERIC_PATTERN.matcher(type);
        if (n.find()) {
            return new int[]{Integer.parseInt(n.group(1)), Integer.parseInt(n.group(2))};
        }
        return new int[]{DEFAULT_PRECISION, DEFAULT_SCALE};
    }

    @Override
    public Class<ByteBuffer> getConvertedType() {
        return ByteBuffer.class;
    }
}


