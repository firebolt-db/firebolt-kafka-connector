package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessDoubleColumnDataTypeConverterTest {

    private final SchemalessDoubleColumnDataTypeConverter converter = new SchemalessDoubleColumnDataTypeConverter();
    private final TableSchema.Column doubleColumn = new TableSchema.Column("val", "double", Types.DOUBLE, false);

    @ParameterizedTest
    @CsvSource({
            " 42.0 ",
             "" + Long.MAX_VALUE + ""
    })
    void convertsSupportedRepresentations(String value) {
        Double result = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(value), doubleColumn);
        assertEquals(Double.valueOf(value), result.doubleValue(), 0.0000001);
    }

    @Test
    void convertsIntegralNumbers() {
        Double b = converter.toParquetValue(new SchemalessKafkaMessageColumnValue((byte) 7), doubleColumn);
        Double s = converter.toParquetValue(new SchemalessKafkaMessageColumnValue((short) 7), doubleColumn);
        Double i = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(7), doubleColumn);
        Double l = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(7L), doubleColumn);
        assertEquals(0, Double.valueOf(7).compareTo(b));
        assertEquals(0, Double.valueOf(7).compareTo(s));
        assertEquals(0, Double.valueOf(7).compareTo(i));
        assertEquals(0, Double.valueOf(7).compareTo(l));
    }

    @Test
    void convertsFloatingNumbersViaBigDecimalValueOf() {
        Double f = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.5f), doubleColumn);
        Double d = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.5d), doubleColumn);
        assertEquals(0, Double.valueOf(12.5f).compareTo(f));
        assertEquals(0, Double.valueOf(12.5d).compareTo(d));
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("abc"), doubleColumn));
    }

}


