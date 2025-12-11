package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaIntegerBinaryColumnDataTypeConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class SchemaBinaryColumnDataTypeConverterFactoryTest {

    private final SchemaBinaryColumnDataTypeConverterFactory factory =
            new SchemaBinaryColumnDataTypeConverterFactory();

    @ParameterizedTest
    @CsvSource({
            "integer",
            "int",
            "int4"
    })
    void returnsIntegerConverterForIntegerAliases(String dataType) {
        TableSchema.Column col = new TableSchema.Column("count", dataType, java.sql.Types.INTEGER, false);
        BinaryColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemaIntegerBinaryColumnDataTypeConverter.class, converter);
    }
}
