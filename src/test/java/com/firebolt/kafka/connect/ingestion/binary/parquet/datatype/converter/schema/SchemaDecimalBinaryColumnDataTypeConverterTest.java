package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Types;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaDecimalBinaryColumnDataTypeConverterTest {

    private final SchemaDecimalBinaryColumnDataTypeConverter converter = new SchemaDecimalBinaryColumnDataTypeConverter();
    private static final TableSchema.Column decDefault = new TableSchema.Column("amount", "numeric", Types.NUMERIC, false, 38, 9);
    private static final TableSchema.Column decP30S7 = new TableSchema.Column("amount", "numeric", Types.NUMERIC, false, 30, 7);

    @ParameterizedTest
    @CsvSource({
            "12.5",
            "  12.50  ",
            "1e3",
            "-1.2345E-3"
    })
    void acceptsNumericStrings(String s) {
        ByteBuffer result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(s).build(), decDefault);
        BigDecimal expected = new BigDecimal(s.trim());
        BigDecimal decoded = decodeDecimal(result, decDefault.getPrecision(), decDefault.getScale());
        assertEquals(0, expected.compareTo(decoded));
    }

    @Test
    void acceptsBigDecimalWithDefaultPrecisionAndScale() {
        ByteBuffer result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(new BigDecimal("123.4500")).build(), decDefault);
        BigDecimal decoded = decodeDecimal(result, 38, 9);
        assertEquals(0, new BigDecimal("123.4500").compareTo(decoded));
    }

    @Test
    void acceptsBigDecimalWithCustomPrecisionAndScale() {
        ByteBuffer result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(new BigDecimal("123.4500")).build(), decP30S7);
        BigDecimal decoded = decodeDecimal(result, 30, 7);
        assertEquals(0, new BigDecimal("123.4500").compareTo(decoded));
    }

    @Test
    void convertsIntegralNumbers() {
        ByteBuffer b = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value((byte) 7).build(), decDefault);
        ByteBuffer s = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value((short) 7).build(), decDefault);
        ByteBuffer i = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(7).build(), decDefault);
        ByteBuffer l = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(7L).build(), decDefault);
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(b, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(s, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(i, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(l, 38, 9)));
    }

    @Test
    void convertsFloatingNumbersViaBigDecimalValueOf() {
        ByteBuffer f = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(12.5f).build(), decDefault);
        ByteBuffer d = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(12.5d).build(), decDefault);
        assertEquals(0, BigDecimal.valueOf(12.5f).compareTo(decodeDecimal(f, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(12.5d).compareTo(decodeDecimal(d, 38, 9)));
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value("abc").build(), decDefault));
    }

    @Test
    void roundsExcessScaleToTargetScaleHalfUp() {
        String input = "1234.123456789012345678901234567890123";
        ByteBuffer buf = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(input).build(), decDefault);
        BigDecimal decoded = decodeDecimal(buf, 38, 9);
        assertEquals(0, new BigDecimal("1234.123456789").compareTo(decoded));
    }

    private static BigDecimal decodeDecimal(ByteBuffer buf, int precision, int scale) {
        LogicalTypes.Decimal lt = LogicalTypes.decimal(precision, scale);
        Schema schema = lt.addToSchema(Schema.create(Schema.Type.BYTES));
        return new Conversions.DecimalConversion().fromBytes(buf, schema, lt);
    }
}


