package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessArrayColumnDecimalDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessDecimalColumnDataTypeConverter decimalConverter = new SchemalessDecimalColumnDataTypeConverter();
    private final TableSchema.Column arrayDecimalColumn = new TableSchema.Column("amounts", "array(numeric)", Types.ARRAY, false, 30, 7);

    @Test
    void convertsMixedElementsToDecimalBytes() {
        arrayConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalConverter);
        List<Object> input = Arrays.asList("12.5", 7, 3.14d, null);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDecimalColumn);
        // decode and assert with scale=0 rounding HALF_UP
        java.math.BigDecimal d0 = decodeDecimal((ByteBuffer) result.get(0), 30, 7);
        java.math.BigDecimal d1 = decodeDecimal((ByteBuffer) result.get(1), 30, 7);
        java.math.BigDecimal d2 = decodeDecimal((ByteBuffer) result.get(2), 30, 7);
        assertEquals(0, new java.math.BigDecimal("12.5").compareTo(d0));
        assertEquals(0, java.math.BigDecimal.valueOf(7).compareTo(d1));
        assertEquals(0, java.math.BigDecimal.valueOf(3.14d).compareTo(d2));
        assertEquals(null, result.get(3));
    }

    @Test
    void invalidElementCausesFailure() {
        arrayConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalConverter);
        List<Object> input = Arrays.asList("not-a-decimal");
        assertThrows(ColumnConversionFailedException.class,
                () -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDecimalColumn));
    }

    private static java.math.BigDecimal decodeDecimal(ByteBuffer buf, int precision, int scale) {
        org.apache.avro.LogicalTypes.Decimal lt = org.apache.avro.LogicalTypes.decimal(precision, scale);
        org.apache.avro.Schema schema = lt.addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
        return new org.apache.avro.Conversions.DecimalConversion().fromBytes(buf, schema, lt);
    }
}


