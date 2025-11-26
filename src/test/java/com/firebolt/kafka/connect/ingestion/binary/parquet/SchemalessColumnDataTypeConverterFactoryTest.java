package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessArrayColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDecimalColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDoubleColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessRealColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestampColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestamptzColumnDataTypeConverter;
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

    // bigint support is covered elsewhere; focusing on timestamptz additions here

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
            "timestamptz"
    })
    void returnsTimestamptzConverterForTimestamptz(String dataType) {
        TableSchema.Column col = new TableSchema.Column("createdAtZ", dataType, java.sql.Types.TIMESTAMP_WITH_TIMEZONE, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessTimestamptzColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "real"
    })
    void returnsRealConverterForReal(String dataType) {
        TableSchema.Column col = new TableSchema.Column("price", dataType, java.sql.Types.REAL, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessRealColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "array(timestamptz)"
    })
    void returnsArrayConverterForArrayTimestamptz(String dataType) {
        TableSchema.Column col = new TableSchema.Column("eventsTsZ", dataType, java.sql.Types.ARRAY, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessArrayColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "array(real)"
    })
    void returnsArrayConverterForArrayReal(String dataType) {
        TableSchema.Column col = new TableSchema.Column("prices", dataType, java.sql.Types.ARRAY, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessArrayColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "double"
    })
    void returnsDoubleConverterForDouble(String dataType) {
        TableSchema.Column col = new TableSchema.Column("ratio", dataType, java.sql.Types.DOUBLE, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessDoubleColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "array(double)"
    })
    void returnsArrayConverterForArrayDouble(String dataType) {
        TableSchema.Column col = new TableSchema.Column("ratios", dataType, java.sql.Types.ARRAY, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessArrayColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "decimal", "numeric"
    })
    void returnsDecimalConverterForDecimal(String dataType) {
        TableSchema.Column col = new TableSchema.Column("amount", dataType, java.sql.Types.NUMERIC, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessDecimalColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "array(decimal)"
    })
    void returnsArrayConverterForArrayDecimal(String dataType) {
        TableSchema.Column col = new TableSchema.Column("amounts", dataType, java.sql.Types.ARRAY, false);
        ColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemalessArrayColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "bytea"
    })
    void throwsForUnsupportedTypes(String dataType) {
        TableSchema.Column col = new TableSchema.Column("c", dataType, java.sql.Types.OTHER, true);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> factory.getConverter(col));
        assertTrue(ex.getMessage().contains("Column type is not yet supported"));
        assertTrue(ex.getMessage().contains(dataType));
        assertTrue(ex.getMessage().contains("c"));
    }
}


