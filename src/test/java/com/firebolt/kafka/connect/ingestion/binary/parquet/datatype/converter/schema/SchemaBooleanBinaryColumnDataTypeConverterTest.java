package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaBooleanBinaryColumnDataTypeConverterTest {

    private final SchemaBooleanBinaryColumnDataTypeConverter converter = new SchemaBooleanBinaryColumnDataTypeConverter();
    private final TableSchema.Column boolCol = new TableSchema.Column("b", "boolean", Types.BOOLEAN, false);

    @Test
    void passesBoolean() {
        Boolean v = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.BOOLEAN)
                .value(Boolean.TRUE)
                .build(), boolCol);
        assertEquals(Boolean.TRUE, v);
    }

    @ParameterizedTest
    @CsvSource({"true,TRUE", "false,FALSE", "t,TRUE", "f,FALSE", "1,TRUE", "0,FALSE"})
    void parsesStrings(String input, String expected) {
        Boolean v = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(input)
                .build(), boolCol);
        assertEquals(Boolean.valueOf(expected), v);
    }

    @ParameterizedTest
    @CsvSource({"1,TRUE", "0,FALSE"})
    void parsesNumbers(long n, String expected) {
        Boolean v = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT64)
                .value(n)
                .build(), boolCol);
        assertEquals(Boolean.valueOf(expected), v);
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class, () ->
                converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                        .schemaType(Schema.Type.STRING)
                        .value("maybe")
                        .build(), boolCol)
        );
    }
}


