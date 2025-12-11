package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Types;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaBigIntBinaryColumnDataTypeConverterTest {

    private final SchemaBigIntBinaryBinaryColumnDataTypeConverter converter = new SchemaBigIntBinaryBinaryColumnDataTypeConverter();

    static Stream<Object[]> validValues() {
        return Stream.of(
                new Object[]{(byte) 1, 1L},
                new Object[]{(short) 2, 2L},
                new Object[]{3, 3L},
                new Object[]{4L, 4L},
                new Object[]{"  5  ", 5L}
        );
    }

    @ParameterizedTest
    @MethodSource("validValues")
    void convertsVariousNumericAndStringInputs(Object input, Long expected) {
        TableSchema.Column col = new TableSchema.Column("count64", "bigint", Types.BIGINT, false);
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder()
                .value(input)
                .build();
        Long result = converter.toParquetValue(value, col);
        assertEquals(expected, result);
    }

    @Test
    void throwsForInvalidString() {
        TableSchema.Column col = new TableSchema.Column("count64", "bigint", Types.BIGINT, false);
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder()
                .value("not-a-number")
                .build();
        assertThrows(ColumnConversionFailedException.class, () -> converter.toParquetValue(value, col));
    }
}

