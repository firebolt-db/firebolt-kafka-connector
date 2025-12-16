package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaBigIntBinaryBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaIntegerBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaRealBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTimestampBinaryColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema.SchemaTimestamptzBinaryColumnDataTypeConverter;
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

    @ParameterizedTest
    @CsvSource({
            "bigint",
            "long",
            "int8"
    })
    void returnsBigIntConverterForBigIntAliases(String dataType) {
        TableSchema.Column col = new TableSchema.Column("count64", dataType, java.sql.Types.BIGINT, false);
        BinaryColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemaBigIntBinaryBinaryColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "timestamp",
            "TIMESTAMP",
            "Timestamp"
    })
    void returnsTimestampConverterForTimestampAliases(String dataType) {
        TableSchema.Column col = new TableSchema.Column("ts", dataType, java.sql.Types.TIMESTAMP, false);
        BinaryColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemaTimestampBinaryColumnDataTypeConverter.class, converter);
    }

    @ParameterizedTest
    @CsvSource({
            "timestamptz",
            "TIMESTAMPTZ",
            "TimeStampTz"
    })
    void returnsTimestamptzConverterForAliases(String dataType) {
        TableSchema.Column col = new TableSchema.Column("tz", dataType, java.sql.Types.TIMESTAMP_WITH_TIMEZONE, false);
        BinaryColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
        assertInstanceOf(SchemaTimestamptzBinaryColumnDataTypeConverter.class, converter);
    }

	@ParameterizedTest
	@CsvSource({
			"real",
			"REAL",
			"float4"
	})
	void returnsRealConverterForAliases(String dataType) {
		TableSchema.Column col = new TableSchema.Column("amount", dataType, java.sql.Types.REAL, false);
		BinaryColumnDataTypeConverter<?, ?> converter = factory.getConverter(col);
		assertInstanceOf(SchemaRealBinaryColumnDataTypeConverter.class, converter);
	}
}
