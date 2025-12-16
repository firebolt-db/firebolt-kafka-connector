package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessBooleanBinaryColumnDataTypeConverterTest {

    private final SchemalessBooleanBinaryColumnDataTypeConverter converter = new SchemalessBooleanBinaryColumnDataTypeConverter();
    private final TableSchema.Column boolCol = new TableSchema.Column("flag", "boolean", Types.BOOLEAN, false);

    @ParameterizedTest
    @CsvSource({
            "true,true",
            "false,false",
            "TRUE,true",
            "FALSE,false",
            "t,true",
            "f,false",
            "T,true",
            "F,false",
            "1,true",
            "0,false"
    })
    void acceptsStringBooleans(String input, boolean expected) {
        Boolean res = converter.toParquetValue(new com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue(input), this.boolCol);
        assertEquals(expected, res);
    }

    @ParameterizedTest
    @CsvSource({
            "true,true",
            "false,false"
    })
    void acceptsBoolean(Boolean input, boolean expected) {
        Boolean res = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), this.boolCol);
        assertEquals(expected, res);
    }

    @ParameterizedTest
    @CsvSource({
            "1,true",
            "0,false"
    })
    void acceptsNumericZeroOne(long input, boolean expected) {
        Boolean res = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), this.boolCol);
        assertEquals(expected, res);
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("yes"), boolCol));
    }
}


