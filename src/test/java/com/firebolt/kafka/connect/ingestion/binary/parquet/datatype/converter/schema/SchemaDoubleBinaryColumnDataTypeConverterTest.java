package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaDoubleBinaryColumnDataTypeConverterTest {

    private final SchemaDoubleBinaryColumnDataTypeConverter converter = new SchemaDoubleBinaryColumnDataTypeConverter();
    private final TableSchema.Column doubleColumn = new TableSchema.Column("amount", "double", Types.DOUBLE, false);

    @ParameterizedTest
    @CsvSource({
            " 12.5 ",
            "" + Double.MAX_VALUE + "",
            "" + Double.MIN_VALUE + ""
    })
    void convertsSupportedStringRepresentations(String value) {
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(Double.parseDouble(value.trim()), result.doubleValue(), 0.000001);
    }

    @Test
    void canConvertByteValue() {
        Object value = Byte.MAX_VALUE;
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(Byte.MAX_VALUE, result.doubleValue(), 0.000001);
    }

    @Test
    void canConvertShortValue() {
        Object value = Short.MAX_VALUE;
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(Short.MAX_VALUE, result.doubleValue(), 0.000001);
    }

    @Test
    void canConvertIntValue() {
        Object value = Integer.MAX_VALUE;
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(((Number) value).doubleValue(), result.doubleValue(), 0.000001);
    }

    @Test
    void canConvertLongValue() {
        Object value = Long.MAX_VALUE;
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(((Number) value).doubleValue(), result.doubleValue(), 0.000001);
    }

    @Test
    void canConvertFloatValue() {
        Object value = Float.MAX_VALUE;
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(((Number) value).doubleValue(), result.doubleValue(), 0.000001);
    }

    @Test
    void canConvertDoubleValue() {
        Object value = Double.MAX_VALUE;
        Double result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), doubleColumn);
        assertEquals(Double.MAX_VALUE, result.doubleValue(), 0.000001);
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value("abc").build(), doubleColumn));
    }
}


