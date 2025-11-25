package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessArrayColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBigIntColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestampColumnDataTypeConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemalessColumnDataTypeConverterFactoryTest {

    private final SchemalessColumnDataTypeConverterFactory factory =
            new SchemalessColumnDataTypeConverterFactory();

    @ParameterizedTest
    @CsvSource({
            "integer",
            "int",
            "int4"
    })
    void returnsIntegerConverterForIntegerAliases(String dataType) {
        TableSchema.Column col = new TableSchema.Column("count", dataType, java.sql.Types.INTEGER, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessIntegerColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "bigint",
            "int8",
            "long"
    })
    void returnsBigIntConverterForBigIntAliases(String dataType) {
        TableSchema.Column col = new TableSchema.Column("count64", dataType, java.sql.Types.BIGINT, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessBigIntColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "array(bigint)"
    })
    void returnsArrayConverterForArrayBigInt(String dataType) {
        TableSchema.Column col = new TableSchema.Column("counts", dataType, java.sql.Types.ARRAY, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessArrayColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "timestamp"
    })
    void returnsTimestampConverterForTimestamp(String dataType) {
        TableSchema.Column col = new TableSchema.Column("createdAt", dataType, java.sql.Types.TIMESTAMP, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessTimestampColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "array(timestamp)"
    })
    void returnsArrayConverterForArrayTimestamp(String dataType) {
        TableSchema.Column col = new TableSchema.Column("eventsTs", dataType, java.sql.Types.ARRAY, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessArrayColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "text",
            "numeric",
            "bytea",
            "real"
    })
    void throwsForUnsupportedTypes(String dataType) {
        TableSchema.Column col = new TableSchema.Column("c", dataType, java.sql.Types.OTHER, true);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> factory.getConverter(col));
        assertTrue(ex.getMessage().contains("Column type is not yet supported"));
        assertTrue(ex.getMessage().contains(dataType));
        assertTrue(ex.getMessage().contains("c"));
    }
}


