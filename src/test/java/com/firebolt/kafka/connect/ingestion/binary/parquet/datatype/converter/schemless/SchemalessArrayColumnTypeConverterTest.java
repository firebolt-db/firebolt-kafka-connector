package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter.parseIsoLocalDateTime;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessArrayColumnTypeConverterTest {

	@Test
	void convertsMixedElementsToMicros() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessTimestampColumnDataTypeConverter tsConverter = new SchemalessTimestampColumnDataTypeConverter();
		TableSchema.Column arrayTsColumn = new TableSchema.Column("timestamps", "array(timestamp)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.TIMESTAMP, tsConverter);
		String iso = "2025-01-02T00:00:00";
		long expectedFromIso = toMicros(parseIsoLocalDateTime(iso));
		long millis = 1_700_000_000_000L;
		long micros = 1_700_000_000_000_000L;

		List<Object> input = Arrays.asList(iso, millis, null, micros);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayTsColumn);
		assertEquals(Arrays.asList(expectedFromIso, millis * 1_000L, null, micros), result);
	}

	@Test
	void invalidTimestampElementCausesFailure() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessTimestampColumnDataTypeConverter tsConverter = new SchemalessTimestampColumnDataTypeConverter();
		TableSchema.Column arrayTsColumn = new TableSchema.Column("timestamps", "array(timestamp)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.TIMESTAMP, tsConverter);
		List<Object> input = Arrays.asList("not-iso");
		assertThrows(ColumnConversionFailedException.class,
				() -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayTsColumn));
	}

	@Test
	void convertsMixedElementsToStrings() throws Exception {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessTextColumnDataTypeConverter textConverter = new SchemalessTextColumnDataTypeConverter();
		TableSchema.Column arrayTextColumn = new TableSchema.Column("txts", "array(text)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.TEXT, textConverter);

		Map<String, Object> m = new HashMap<>();
		m.put("a", 1);
		m.put("b", "x");

		List<Object> input = Arrays.asList("s", 7, m, null);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayTextColumn);

		ObjectMapper om = new ObjectMapper();
		@SuppressWarnings("unchecked")
		Map<String, Object> back = om.readValue((String) result.get(2), Map.class);
		assertEquals("s", result.get(0));
		assertEquals("7", result.get(1));
		assertEquals(m, back);
		assertEquals(null, result.get(3));
	}

	@Test
	void convertsMixedElementsToFloats() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessRealColumnDataTypeConverter realConverter = new SchemalessRealColumnDataTypeConverter();
		TableSchema.Column arrayRealColumn = new TableSchema.Column("amounts", "array(real)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.REAL, realConverter);
		List<Object> input = Arrays.asList(1, 2L, 3.5d, 4.25f, "5.5", " 6.75 ", null);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayRealColumn);
		assertEquals(Arrays.asList(1.0f, 2.0f, 3.5f, 4.25f, 5.5f, 6.75f, null), result);
	}

	@Test
	void invalidRealElementCausesFailure() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessRealColumnDataTypeConverter realConverter = new SchemalessRealColumnDataTypeConverter();
		TableSchema.Column arrayRealColumn = new TableSchema.Column("amounts", "array(real)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.REAL, realConverter);
		List<Object> input = Arrays.asList("not-a-float");
		assertThrows(ColumnConversionFailedException.class,
				() -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayRealColumn));
	}

	@Test
	void convertsMixedElementsToDoubles() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessDoubleColumnDataTypeConverter doubleConverter = new SchemalessDoubleColumnDataTypeConverter();
		TableSchema.Column arrayDoubleColumn = new TableSchema.Column("vals", "array(double precision)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.DOUBLE, doubleConverter);
		List<Object> input = Arrays.asList(1, 2L, 3.5d, 4.25f, "5.5", " 6.75 ", null);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDoubleColumn);
		assertEquals(Arrays.asList(1.0d, 2.0d, 3.5d, 4.25d, 5.5d, 6.75d, null), result);
	}

	@Test
	void convertsMixedElementsToDecimalBytes() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessDecimalColumnDataTypeConverter decimalConverter = new SchemalessDecimalColumnDataTypeConverter();
		TableSchema.Column arrayDecimalColumn = new TableSchema.Column("amounts", "array(numeric)", Types.ARRAY, false,30, 7);

		arrayConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalConverter);
		List<Object> input = Arrays.asList("12.5", 7, 3.14d, null);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDecimalColumn);
		java.math.BigDecimal d0 = decodeDecimal((ByteBuffer) result.get(0), 30, 7);
		java.math.BigDecimal d1 = decodeDecimal((ByteBuffer) result.get(1), 30, 7);
		java.math.BigDecimal d2 = decodeDecimal((ByteBuffer) result.get(2), 30, 7);
		assertEquals(0, new java.math.BigDecimal("12.5").compareTo(d0));
		assertEquals(0, java.math.BigDecimal.valueOf(7).compareTo(d1));
		assertEquals(0, java.math.BigDecimal.valueOf(3.14d).compareTo(d2));
		assertEquals(null, result.get(3));
	}

	@Test
	void invalidDecimalElementCausesFailure() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessDecimalColumnDataTypeConverter decimalConverter = new SchemalessDecimalColumnDataTypeConverter();
		TableSchema.Column arrayDecimalColumn = new TableSchema.Column("amounts", "array(numeric)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalConverter);
		List<Object> input = Arrays.asList("not-a-decimal");
		assertThrows(ColumnConversionFailedException.class,
				() -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDecimalColumn));
	}

	@Test
	void convertsMixedElementsToDaysSinceEpoch() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessDateColumnDataTypeConverter dateConverter = new SchemalessDateColumnDataTypeConverter();
		TableSchema.Column arrayDateColumn = new TableSchema.Column("ds", "array(date)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.DATE, dateConverter);
		String s = "2025-01-02";
		int days = (int) LocalDate.parse(s).toEpochDay();
		List<Object> input = Arrays.asList(s, 19700, null);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDateColumn);
		assertEquals(Arrays.asList(days, 19700, null), result);
	}

	@Test
	void convertsMixedByteaElements() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessByteaColumnDataTypeConverter byteaConverter = new SchemalessByteaColumnDataTypeConverter();
		TableSchema.Column arrayCol = new TableSchema.Column("bytes", "array(bytea)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.BYTEA, byteaConverter);
		List<Object> input = Arrays.asList(
				"hi",
				"a".getBytes(StandardCharsets.UTF_8),
				ByteBuffer.wrap("Z".getBytes(StandardCharsets.UTF_8)),
				null
		);
		List<?> out = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayCol);
		assertEquals(4, out.size());
		assertArrayEquals("hi".getBytes(StandardCharsets.UTF_8), getBytes((ByteBuffer) out.get(0)));
		assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), getBytes((ByteBuffer) out.get(1)));
		assertArrayEquals("Z".getBytes(StandardCharsets.UTF_8), getBytes((ByteBuffer) out.get(2)));
		assertEquals(null, out.get(3));
	}

	@Test
	void convertsMixedElementsToBooleans() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessBooleanColumnDataTypeConverter boolConverter = new SchemalessBooleanColumnDataTypeConverter();
		TableSchema.Column arrayBoolCol = new TableSchema.Column("flags", "array(boolean)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.BOOLEAN, boolConverter);
		List<Object> in = Arrays.asList("true", "FALSE", 1, 0, true, false, null, "t", "f");
		List<?> out = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(in), arrayBoolCol);
		assertEquals(Arrays.asList(true, false, true, false, true, false, null, true, false), out);
	}

	@Test
	void invalidBooleanElementCausesFailure() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessBooleanColumnDataTypeConverter boolConverter = new SchemalessBooleanColumnDataTypeConverter();
		TableSchema.Column arrayBoolCol = new TableSchema.Column("flags", "array(boolean)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.BOOLEAN, boolConverter);
		List<Object> in = Arrays.asList("maybe");
		assertThrows(ColumnConversionFailedException.class,
				() -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(in), arrayBoolCol));
	}

	@Test
	void convertsMixedNumericElementsToLongs() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessBigIntColumnDataTypeConverter bigIntConverter = new SchemalessBigIntColumnDataTypeConverter();
		TableSchema.Column arrayBigintColumn = new TableSchema.Column("counts", "array(bigint)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.BIGINT, bigIntConverter);
		List<Object> input = Arrays.asList(1, 2L, "3", " 4 ", null, (short) 5, (byte) 6);
		List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayBigintColumn);
		assertEquals(Arrays.asList(1L, 2L, 3L, 4L, null, 5L, 6L), result);
	}

	@Test
	void invalidBigintElementCausesFailure() {
		SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
		SchemalessBigIntColumnDataTypeConverter bigIntConverter = new SchemalessBigIntColumnDataTypeConverter();
		TableSchema.Column arrayBigintColumn = new TableSchema.Column("counts", "array(bigint)", Types.ARRAY, false);

		arrayConverter.addConverter(FireboltColumnDataType.BIGINT, bigIntConverter);
		List<Object> input = Arrays.asList(1, "not-a-number", 3);
		assertThrows(ColumnConversionFailedException.class,
				() -> arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayBigintColumn));
	}

	private static long toMicros(LocalDateTime ldt) {
		Instant instant = ldt.toInstant(ZoneOffset.UTC);
		return instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
	}

	private static java.math.BigDecimal decodeDecimal(ByteBuffer buf, int precision, int scale) {
		org.apache.avro.LogicalTypes.Decimal lt = org.apache.avro.LogicalTypes.decimal(precision, scale);
		org.apache.avro.Schema schema = lt.addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
		return new org.apache.avro.Conversions.DecimalConversion().fromBytes(buf, schema, lt);
	}

	private static byte[] getBytes(ByteBuffer buf) {
		byte[] copy = new byte[buf.remaining()];
		buf.get(copy);
		return copy;
	}
}


