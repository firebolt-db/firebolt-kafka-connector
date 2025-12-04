package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessDecimalColumnDataTypeConverterTest {

    private final SchemalessDecimalColumnDataTypeConverter converter = new SchemalessDecimalColumnDataTypeConverter();
    private static final TableSchema.Column decColumnDefaultPrecisionAndScale = new TableSchema.Column("amount", "decimal", Types.NUMERIC, false, 38, 9);
    private static final TableSchema.Column decColumnDefaultPrecision30AndScale7 = new TableSchema.Column("amount", "decimal", Types.NUMERIC, false, 30, 7);

    @ParameterizedTest
    @MethodSource("decimalAsString")
    void acceptsNumericStrings(String s, TableSchema.Column decimalTableSchemaColumn) {
        ByteBuffer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(s), decimalTableSchemaColumn);
        BigDecimal expected = new BigDecimal(s.trim());
        BigDecimal decoded = decodeDecimal(result, decimalTableSchemaColumn.getPrecision(), decimalTableSchemaColumn.getScale());
        assertEquals(0, expected.compareTo(decoded));
    }

    @Test
    void acceptsBigDecimalWithDefaultPrecisionAndScale() {
        ByteBuffer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(new BigDecimal("123.4500")), decColumnDefaultPrecisionAndScale);
        BigDecimal decoded = decodeDecimal(result, 38, 9);
        assertEquals(0, new BigDecimal("123.4500").compareTo(decoded));
    }

    @Test
    void acceptsBigDecimalWithCustomPrecisionAndScale() {
        ByteBuffer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(new BigDecimal("123.4500")), decColumnDefaultPrecision30AndScale7);
        BigDecimal decoded = decodeDecimal(result, 30, 7);
        assertEquals(0, new BigDecimal("123.4500").compareTo(decoded));
    }

    @Test
    void convertsIntegralNumbers() {
        ByteBuffer b = converter.toParquetValue(new SchemalessKafkaMessageColumnValue((byte) 7), decColumnDefaultPrecisionAndScale);
        ByteBuffer s = converter.toParquetValue(new SchemalessKafkaMessageColumnValue((short) 7), decColumnDefaultPrecisionAndScale);
        ByteBuffer i = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(7), decColumnDefaultPrecisionAndScale);
        ByteBuffer l = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(7L), decColumnDefaultPrecisionAndScale);
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(b, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(s, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(i, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(l, 38, 9)));
    }

    @Test
    void convertsFloatingNumbersViaBigDecimalValueOf() {
        ByteBuffer f = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.5f), decColumnDefaultPrecisionAndScale);
        ByteBuffer d = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.5d), decColumnDefaultPrecisionAndScale);
        assertEquals(0, BigDecimal.valueOf(12.5f).compareTo(decodeDecimal(f, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(12.5d).compareTo(decodeDecimal(d, 38, 9)));
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("abc"), decColumnDefaultPrecisionAndScale));
    }

    @Test
    void roundsExcessScaleToTargetScaleHalfUp() {
        String input = "1234.123456789012345678901234567890123"; // scale 33
        ByteBuffer buf = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), decColumnDefaultPrecisionAndScale); // scale 9
        BigDecimal decoded = decodeDecimal(buf, 38, 9);
        assertEquals(0, new BigDecimal("1234.123456789").compareTo(decoded));
    }

    private static BigDecimal decodeDecimal(ByteBuffer buf, int precision, int scale) {
        LogicalTypes.Decimal lt = LogicalTypes.decimal(precision, scale);
        Schema schema = lt.addToSchema(Schema.create(Schema.Type.BYTES));
        return new Conversions.DecimalConversion().fromBytes(buf, schema, lt);
    }

    protected static Stream<Arguments> decimalAsString() {
        return Stream.of(
                Arguments.of( "12.5", decColumnDefaultPrecisionAndScale),
                Arguments.of( "  12.50  ", decColumnDefaultPrecisionAndScale),
                Arguments.of( "1e3", decColumnDefaultPrecisionAndScale),
                Arguments.of( "-1.2345E-3", decColumnDefaultPrecisionAndScale),
                Arguments.of( "12.5", decColumnDefaultPrecision30AndScale7),
                Arguments.of( "  12.50  ", decColumnDefaultPrecision30AndScale7),
                Arguments.of( "1e3", decColumnDefaultPrecision30AndScale7),
                Arguments.of( "-1.2345E-3", decColumnDefaultPrecision30AndScale7)
        );
    }
}


