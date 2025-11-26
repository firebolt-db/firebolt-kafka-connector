package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessDecimalColumnDataTypeConverterTest {

    private final SchemalessDecimalColumnDataTypeConverter converter = new SchemalessDecimalColumnDataTypeConverter();
    private final TableSchema.Column decColumn = new TableSchema.Column("amount", "decimal", Types.NUMERIC, false);

    @ParameterizedTest
    @CsvSource({
            "12.5",
            "  12.50  ",
            "1e3",
            "-1.2345E-3"
    })
    void acceptsNumericStrings(String s) {
        ByteBuffer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(s), decColumn);
        BigDecimal expected = new BigDecimal(s.trim());
        BigDecimal decoded = decodeDecimal(result, 38, 9);
        assertEquals(0, expected.compareTo(decoded));
    }

    @Test
    void acceptsBigDecimal() {
        ByteBuffer result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(new BigDecimal("123.4500")), decColumn);
        BigDecimal decoded = decodeDecimal(result, 38, 9);
        assertEquals(0, new BigDecimal("123.4500").compareTo(decoded));
    }

    @Test
    void convertsIntegralNumbers() {
        ByteBuffer b = converter.toParquetValue(new SchemalessKafkaMessageColumnValue((byte) 7), decColumn);
        ByteBuffer s = converter.toParquetValue(new SchemalessKafkaMessageColumnValue((short) 7), decColumn);
        ByteBuffer i = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(7), decColumn);
        ByteBuffer l = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(7L), decColumn);
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(b, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(s, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(i, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(decodeDecimal(l, 38, 9)));
    }

    @Test
    void convertsFloatingNumbersViaBigDecimalValueOf() {
        ByteBuffer f = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.5f), decColumn);
        ByteBuffer d = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.5d), decColumn);
        assertEquals(0, BigDecimal.valueOf(12.5f).compareTo(decodeDecimal(f, 38, 9)));
        assertEquals(0, BigDecimal.valueOf(12.5d).compareTo(decodeDecimal(d, 38, 9)));
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("abc"), decColumn));
    }

    private static BigDecimal decodeDecimal(ByteBuffer buf, int precision, int scale) {
        LogicalTypes.Decimal lt = LogicalTypes.decimal(precision, scale);
        Schema schema = lt.addToSchema(Schema.create(Schema.Type.BYTES));
        return new Conversions.DecimalConversion().fromBytes(buf, schema, lt);
    }
}


