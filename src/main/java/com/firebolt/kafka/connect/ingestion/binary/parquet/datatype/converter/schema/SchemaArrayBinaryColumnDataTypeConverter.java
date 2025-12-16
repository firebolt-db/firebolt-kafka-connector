package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.BinaryColumnDataTypeConverter;
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
		private static final String REAL_TYPE_NAME = "real";

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
		} else if (typeName.equals(REAL_TYPE_NAME)) {
			return asRealArray(elements, tableColumn, schemaKafkaMessageColumnValue.getSchemaType(), schemaKafkaMessageColumnValue.getSchemaSubType(), schemaKafkaMessageColumnValue.getSchemaTypeParams());
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
		} else if (fireboltColumn.getDataType().equals("array(real)")) {
			return REAL_TYPE_NAME;
        }

        // add more data types
        return "string";
    }

}


