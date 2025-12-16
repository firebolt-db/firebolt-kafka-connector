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

class SchemaRealBinaryColumnDataTypeConverterTest {

	private final SchemaRealBinaryColumnDataTypeConverter converter = new SchemaRealBinaryColumnDataTypeConverter();
	private final TableSchema.Column realColumn = new TableSchema.Column("amount", "real", Types.REAL, false);

	@ParameterizedTest
	@CsvSource({
			" 12.5 ",
			"" + Float.MAX_VALUE + "",
			"" + Float.MIN_VALUE + "",
	})
	void convertsSupportedStringRepresentations(String value) {
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		assertEquals(Float.parseFloat(value.trim()), result.floatValue(), 0.000001);
	}

	@Test
	void canConvertByteValue() {
		Object value = Byte.MAX_VALUE;
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		assertEquals(Byte.MAX_VALUE, result.floatValue(), 0.000001);
	}

	@Test
	void canConvertShortValue() {
		Object value = Short.MAX_VALUE;
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		assertEquals(Short.MAX_VALUE, result.floatValue(), 0.000001);
	}

	@Test
	void canConvertIntValue() {
		Object value = Integer.MAX_VALUE;
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		float expected = ((Number) value).floatValue();
		assertEquals(expected, result.floatValue(), 0.000001);
	}

	@Test
	void canConvertLongValue() {
		Object value = Long.MAX_VALUE;
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		float expected = ((Number) value).floatValue();
		assertEquals(expected, result.floatValue(), 0.000001);
	}

	@Test
	void canConvertFloatValue() {
		Object value = Float.MAX_VALUE;
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		assertEquals(Float.MAX_VALUE, result.floatValue(), 0.000001);
	}

	@Test
	void canConvertDoubleValueWhenDoesNotExceedFloatMax() {
		Object value = Double.valueOf(Float.MAX_VALUE);
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		assertEquals(Float.MAX_VALUE, result.floatValue(), 0.000001);
	}

	@Test
	void canConvertDoubleValueWhenIsNotLessThanNegativeFloatMax() {
		Object value = Double.valueOf(-Float.MAX_VALUE);
		Float result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(value).build(), realColumn);
		assertEquals(-Float.MAX_VALUE, result.floatValue(), 0.000001);
	}

	@Test
	void cannotConvertDoubleValueWhenItDoesNotFitInFloat() {
		assertThrows(ColumnConversionFailedException.class,
				() -> converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value(Double.MAX_VALUE).build(), realColumn));
	}

	@Test
	void invalidStringThrows() {
		assertThrows(ColumnConversionFailedException.class,
				() -> converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().value("abc").build(), realColumn));
	}
}


