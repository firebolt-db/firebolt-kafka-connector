package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.BinaryColumnDataTypeConverter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;

@Slf4j
public class SchemaArrayBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue, List<? extends Object>> {

    private static final String INTEGER_TYPE_NAME = "integer";
    private static final String BIGINT_TYPE_NAME = "bigint";
    private static final String TIMESTAMP_TYPE_NAME = "timestamp";
    private static final String TIMESTAMPTZ_TYPE_NAME = "timestamptz";
    private static final String TEXT_TYPE_NAME = "string";
    private static final String DATE_TYPE_NAME = "date";
    private static final String BOOLEAN_TYPE_NAME = "boolean";
    private static final String REAL_TYPE_NAME = "real";
    private static final String DOUBLE_TYPE_NAME = "double";
    private static final String DECIMAL_TYPE_NAME = "numeric";
    private static final String BYTEA_TYPE_NAME = "bytea";

    private Map<FireboltColumnDataType, BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, ?>> converters = new HashMap<>();

    @Override
    public List<? extends Object> toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column tableColumn) throws ColumnConversionFailedException {
        List<?> elements = (List<?>) schemaKafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(tableColumn);
        if (CollectionUtils.isEmpty(elements)) {
            return Collections.emptyList();
        }

        // jdbc driver is not creating timestamps but array[integers] since the values are coming as ints
        if (typeName.equals(INTEGER_TYPE_NAME)) {
            return asIntArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(BIGINT_TYPE_NAME)) {
            return asLongArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(TIMESTAMP_TYPE_NAME)) {
            return asTimestampArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
		} else if (typeName.equals(TIMESTAMPTZ_TYPE_NAME)) {
            return asTimestamptzArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(DATE_TYPE_NAME)) {
            return asDateArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(TEXT_TYPE_NAME)) {
            return asTextArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(BOOLEAN_TYPE_NAME)) {
            return asBooleanArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
		} else if (typeName.equals(REAL_TYPE_NAME)) {
			return asRealArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(DOUBLE_TYPE_NAME)) {
            return asDoubleArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(DECIMAL_TYPE_NAME)) {
            return asDecimalArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        } else if (typeName.equals(BYTEA_TYPE_NAME)) {
            return asByteaArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
        }

        log.warn("Could not resolve type name: {}", typeName);
        return Collections.emptyList();
    }

    @Override
    public Class<List<? extends Object>> getConvertedType() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<List<? extends Object>> clazz = (Class) List.class;
        return clazz;
    }

    public <R> void addConverter(FireboltColumnDataType type, BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, R> converter) {
        converters.put(type, converter);
    }

    // NOTE once this https://packboard.atlassian.net/browse/FIR-50959 we need to check the inner data type rather than array(integer)
    // as this is should be the inner table column not the outer one
    private List<? extends Object> asIntArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<Integer> integers = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer>) converters.get(FireboltColumnDataType.INTEGER);

        for (Object element : elements) {
            if (element == null) {
                integers.add(null);
            } else {
                Integer convertedValue = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().schemaType(schemaType).schemaSubType(schemaSubType).value(element).build(), tableColumn);
                integers.add(convertedValue);
            }
        }
        return integers;
    }

    private List<? extends Object> asLongArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<Long> longs = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long>) converters.get(FireboltColumnDataType.BIGINT);

        for (Object element : elements) {
            if (element == null) {
                longs.add(null);
            } else {
                Long convertedValue = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder().schemaType(schemaType).schemaSubType(schemaSubType).value(element).build(), tableColumn);
                longs.add(convertedValue);
            }
        }
        return longs;
    }

    private List<? extends Object> asTimestampArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<Long> longs = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long>) converters.get(FireboltColumnDataType.TIMESTAMP);

        for (Object element : elements) {
            if (element == null) {
                longs.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaSubType) //schema type is always array. So pass in the schema subtype since it needs to parse each element
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();

                Long convertedValue = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                longs.add(convertedValue);
            }
        }
        return longs;
    }

    private List<? extends Object> asTimestamptzArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<Long> longs = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Long>) converters.get(FireboltColumnDataType.TIMESTAMPTZ);

        for (Object element : elements) {
            if (element == null) {
                longs.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaSubType)
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();

                Long convertedValue = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                longs.add(convertedValue);
            }
        }
        return longs;
    }

    private List<? extends Object> asDateArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<Integer> days = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer>) converters.get(FireboltColumnDataType.DATE);

        for (Object element : elements) {
            if (element == null) {
                days.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaSubType)
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();

                Integer convertedValue = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                days.add(convertedValue);
            }
        }
        return days;
    }

    private List<? extends Object> asTextArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<String> strings = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, String> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, String>) converters.get(FireboltColumnDataType.TEXT);

        for (Object element : elements) {
            if (element == null) {
                strings.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaType)
                        .schemaSubType(schemaSubType)
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();
                String converted = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                strings.add(converted);
            }
        }
        return strings;
    }

    private List<? extends Object> asBooleanArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<Boolean> booleans = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Boolean> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Boolean>) converters.get(FireboltColumnDataType.BOOLEAN);

        for (Object element : elements) {
            if (element == null) {
                booleans.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaType)
                        .schemaSubType(schemaSubType)
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();
                Boolean converted = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                booleans.add(converted);
            }
        }
        return booleans;
    }

	private List<? extends Object> asRealArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
		List<Float> floats = new ArrayList<>();

		@SuppressWarnings("unchecked")
		BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Float> converter =
				(BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Float>) converters.get(FireboltColumnDataType.REAL);

		for (Object element : elements) {
			if (element == null) {
				floats.add(null);
			} else {
				SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
						.schemaType(schemaType)
						.schemaSubType(schemaSubType)
						.schemaTypeParams(schemaTypeParams)
						.value(element)
						.build();
				Float converted = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
				floats.add(converted);
			}
		}
		return floats;
	}

	private List<? extends Object> asDoubleArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
		List<Double> doubles = new ArrayList<>();

		@SuppressWarnings("unchecked")
		BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Double> converter =
				(BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Double>) converters.get(FireboltColumnDataType.DOUBLE);

		for (Object element : elements) {
			if (element == null) {
				doubles.add(null);
			} else {
				SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
						.schemaType(schemaType)
						.schemaSubType(schemaSubType)
						.schemaTypeParams(schemaTypeParams)
						.value(element)
						.build();
				Double converted = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
				doubles.add(converted);
			}
		}
		return doubles;
	}

    private List<? extends Object> asDecimalArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<ByteBuffer> decimals = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, ByteBuffer> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, ByteBuffer>) converters.get(FireboltColumnDataType.DECIMAL);

        for (Object element : elements) {
            if (element == null) {
                decimals.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaType)
                        .schemaSubType(schemaSubType)
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();
                ByteBuffer converted = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                decimals.add(converted);
            }
        }
        return decimals;
    }

    private List<? extends Object> asByteaArray(List<?> elements, TableSchema.Column tableColumn, Schema.Type schemaType, Schema.Type schemaSubType, Map<String, String> schemaTypeParams) {
        List<ByteBuffer> buffers = new ArrayList<>();

        @SuppressWarnings("unchecked")
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, ByteBuffer> converter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, ByteBuffer>) converters.get(FireboltColumnDataType.BYTEA);

        for (Object element : elements) {
            if (element == null) {
                buffers.add(null);
            } else {
                SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue = SchemaKafkaMessageColumnValue.builder()
                        .schemaType(schemaType)
                        .schemaSubType(schemaSubType)
                        .schemaTypeParams(schemaTypeParams)
                        .value(element)
                        .build();
                ByteBuffer converted = converter.toParquetValue(schemaKafkaMessageColumnValue, tableColumn);
                buffers.add(converted);
            }
        }
        return buffers;
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        // NOTE once this https://packboard.atlassian.net/browse/FIR-50959 we need to check the inner data type rather than array(integer)
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return INTEGER_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(bigint)")) {
            return BIGINT_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(timestamp)")) {
            return TIMESTAMP_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(timestamptz)")) {
            return TIMESTAMPTZ_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(date)")) {
            return DATE_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(text)")) {
            return TEXT_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(boolean)")) {
            return BOOLEAN_TYPE_NAME;
		} else if (fireboltColumn.getDataType().equals("array(real)")) {
			return REAL_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(double precision)")) {
            return DOUBLE_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(numeric)") || fireboltColumn.getDataType().equals("array(decimal)")) {
            return DECIMAL_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(bytea)")) {
            return BYTEA_TYPE_NAME;
        }

        // add more data types
        return "string";
    }

}


