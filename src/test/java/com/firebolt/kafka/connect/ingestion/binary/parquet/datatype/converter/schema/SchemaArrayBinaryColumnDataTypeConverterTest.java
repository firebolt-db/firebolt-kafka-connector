package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.BinaryColumnDataTypeConverter;
import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class SchemaArrayBinaryColumnDataTypeConverterTest {

    @Test
    void convertsIntegerArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();

        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer> intConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.INTEGER, intConverter);

        TableSchema.Column col = new TableSchema.Column("ints", "array(integer)", Types.ARRAY, true);

        List<Object> elements = Arrays.asList(1, 2L, "3", null);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.INT32)
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        when(intConverter.toParquetValue(any(SchemaKafkaMessageColumnValue.class), eq(col)))
                .thenAnswer(inv -> {
                    Object v = ((SchemaKafkaMessageColumnValue) inv.getArgument(0)).getValue();
                    if (v instanceof Number) {
                        return ((Number) v).intValue();
                    }
                    return Integer.parseInt(v.toString());
                });

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertEquals(Arrays.asList(1, 2, 3, null), result);

        ArgumentCaptor<SchemaKafkaMessageColumnValue> captor = ArgumentCaptor.forClass(SchemaKafkaMessageColumnValue.class);
        verify(intConverter, times(3)).toParquetValue(captor.capture(), eq(col));
        List<SchemaKafkaMessageColumnValue> captured = captor.getAllValues();
        assertEquals(3, captured.size());
        assertEquals(1, captured.get(0).getValue());
        assertEquals(2L, captured.get(1).getValue());
        assertEquals("3", captured.get(2).getValue());
        assertTrue(captured.stream().allMatch(v -> v.getSchemaType() == Schema.Type.ARRAY && v.getSchemaSubType() == Schema.Type.INT32));
    }

    @Test
    void convertsBigintArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();

        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long> longConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.BIGINT, longConverter);

        TableSchema.Column col = new TableSchema.Column("longs", "array(bigint)", Types.ARRAY, true);

        List<Object> elements = Arrays.asList(1, 2L, "3", null);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.INT64)
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        when(longConverter.toParquetValue(any(SchemaKafkaMessageColumnValue.class), eq(col)))
                .thenAnswer(inv -> {
                    Object v = ((SchemaKafkaMessageColumnValue) inv.getArgument(0)).getValue();
                    if (v == null) return null;
                    if (v instanceof Number) {
                        return ((Number) v).longValue();
                    }
                    return Long.parseLong(v.toString());
                });

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertEquals(Arrays.asList(1L, 2L, 3L, null), result);
    }

    @Test
    void returnsEmptyListWhenElementsEmpty() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer> intConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.INTEGER, intConverter);

        TableSchema.Column col = new TableSchema.Column("ints", "array(integer)", Types.ARRAY, true);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.INT32)
                .schemaTypeParams(Map.of())
                .value(List.of())
                .build();

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertTrue(result.isEmpty());
        verifyNoInteractions(intConverter);
    }

    @Test
    void convertsTimestampArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long> tsConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.TIMESTAMP, tsConverter);

        TableSchema.Column col = new TableSchema.Column("ts_arr", "array(timestamp)", Types.ARRAY, true);

        List<Object> elements = Arrays.asList(1_700_000_000_000L, 1_700_000_000_000_000L, "2025-01-02T03:04:05", null);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.INT64)
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        when(tsConverter.toParquetValue(any(SchemaKafkaMessageColumnValue.class), eq(col)))
                .thenAnswer(inv -> {
                    Object v = ((SchemaKafkaMessageColumnValue) inv.getArgument(0)).getValue();
                    if (v == null) return null;
                    if (v instanceof Number) {
                        long n = ((Number) v).longValue();
                        return n > 10_000_000_000_000L ? n : n * 1_000L;
                    }
                    // return a known micros value for the example string
                    long seconds = 1735787045L; // 2025-01-02T03:04:05Z
                    return seconds * 1_000_000L;
                });

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertEquals(Arrays.asList(1_700_000_000_000_000L, 1_700_000_000_000_000L, 1_735_787_045_000_000L, null), result);
    }

    @Test
    void convertsTimestamptzArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long> tzConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.TIMESTAMPTZ, tzConverter);

        TableSchema.Column col = new TableSchema.Column("tz_arr", "array(timestamptz)", Types.ARRAY, true);

        List<Object> elements = Arrays.asList(
                1_700_000_000_000L,               // millis
                1_700_000_000_000_000L,           // micros
                "2025-01-02T03:04:05Z",           // ISO Z
                "2025-01-02 05:04:05+02:00",      // offset
                null
        );
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRING) // elements can be strings for timestamptz
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        when(tzConverter.toParquetValue(any(SchemaKafkaMessageColumnValue.class), eq(col)))
                .thenAnswer(inv -> {
                    Object v = ((SchemaKafkaMessageColumnValue) inv.getArgument(0)).getValue();
                    if (v == null) return null;
                    if (v instanceof Number) {
                        long n = ((Number) v).longValue();
                        return n > 10_000_000_000_000L ? n : n * 1_000L;
                    }
                    // for strings return deterministic micros for assertion
                    if ("2025-01-02T03:04:05Z".equals(v)) {
                        return 1_735_787_045_000_000L;
                    } else {
                        // 05:04:05+02:00 == 03:04:05Z
                        return 1_735_787_045_000_000L;
                    }
                });

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertEquals(Arrays.asList(
                1_700_000_000_000_000L,
                1_700_000_000_000_000L,
                1_735_787_045_000_000L,
                1_735_787_045_000_000L,
                null
        ), result);
    }

	@Test
	void convertsRealArrayElements() {
		SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();
		SchemaRealBinaryColumnDataTypeConverter realConverter = new SchemaRealBinaryColumnDataTypeConverter();
		arrayConverter.addConverter(FireboltColumnDataType.REAL, realConverter);

		TableSchema.Column col = new TableSchema.Column("reals", "array(real)", Types.ARRAY, true);

		List<Object> elements = Arrays.asList(1, 2.5d, "3.75", null, Float.valueOf(4.5f));
		SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
				.schemaType(Schema.Type.ARRAY)
				.schemaSubType(Schema.Type.FLOAT32)
				.schemaTypeParams(Collections.emptyMap())
				.value(elements)
				.build();

		List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
		assertEquals(Arrays.asList(1.0f, 2.5f, 3.75f, null, 4.5f), result);
	}

	@Test
	void convertsDoubleArrayElements() {
		SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();
		SchemaDoubleBinaryColumnDataTypeConverter doubleConverter = new SchemaDoubleBinaryColumnDataTypeConverter();
		arrayConverter.addConverter(FireboltColumnDataType.DOUBLE, doubleConverter);

		TableSchema.Column col = new TableSchema.Column("doubles", "array(double precision)", Types.ARRAY, true);

		List<Object> elements = Arrays.asList(1, 2.5d, "3.75", null, Float.valueOf(4.5f));
		SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
				.schemaType(Schema.Type.ARRAY)
				.schemaSubType(Schema.Type.FLOAT64)
				.schemaTypeParams(Collections.emptyMap())
				.value(elements)
				.build();

		List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
		assertEquals(Arrays.asList(1.0d, 2.5d, 3.75d, null, 4.5d), result);
	}

    @Test
    void convertsDecimalArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();
        SchemaDecimalBinaryColumnDataTypeConverter decimalConverter = new SchemaDecimalBinaryColumnDataTypeConverter();
        arrayConverter.addConverter(FireboltColumnDataType.DECIMAL, decimalConverter);

        TableSchema.Column col = new TableSchema.Column("amounts", "array(numeric)", Types.ARRAY, false, 30, 7);

        List<Object> elements = Arrays.asList("12.5", 7, 3.14d, null);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRING)
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        BigDecimal d0 = decodeDecimal((ByteBuffer) result.get(0), 30, 7);
        BigDecimal d1 = decodeDecimal((ByteBuffer) result.get(1), 30, 7);
        BigDecimal d2 = decodeDecimal((ByteBuffer) result.get(2), 30, 7);
        assertEquals(0, new BigDecimal("12.5").compareTo(d0));
        assertEquals(0, BigDecimal.valueOf(7).compareTo(d1));
        assertEquals(0, BigDecimal.valueOf(3.14d).compareTo(d2));
        assertEquals(null, result.get(3));
    }

    private static BigDecimal decodeDecimal(ByteBuffer buf, int precision, int scale) {
        org.apache.avro.LogicalTypes.Decimal lt = org.apache.avro.LogicalTypes.decimal(precision, scale);
        org.apache.avro.Schema schema = lt.addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
        return new org.apache.avro.Conversions.DecimalConversion().fromBytes(buf, schema, lt);
    }

    @Test
    void convertsDateArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();
        SchemaDateBinaryColumnDataTypeConverter dateConverter = new SchemaDateBinaryColumnDataTypeConverter();
        arrayConverter.addConverter(FireboltColumnDataType.DATE, dateConverter);

        TableSchema.Column col = new TableSchema.Column("dates", "array(date)", Types.ARRAY, true);

        LocalDate ld1 = LocalDate.of(2023, 1, 2);
        Date utilDate = Date.from(ld1.atStartOfDay(ZoneOffset.UTC).toInstant());

        List<Object> elements = Arrays.asList("2023-01-01", utilDate, 5, null);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.STRING)
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertEquals(Arrays.asList(
                (int) LocalDate.of(2023, 1, 1).toEpochDay(),
                (int) ld1.toEpochDay(),
                5,
                null
        ), result);
    }
}

