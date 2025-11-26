package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.ColumnDataTypeConverter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Slf4j
public class SchemalessArrayColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, List<? extends Object>> {

    private static final String INTEGER_TYPE_NAME = "integer";
    private static final String BIGINT_TYPE_NAME = "bigint";
    private static final String TIMESTAMP_TYPE_NAME = "timestamp";
    private static final String TIMESTAMPTZ_TYPE_NAME = "timestamptz";
    private static final String REAL_TYPE_NAME = "real";
    private static final String DOUBLE_TYPE_NAME = "double";
    private static final String DECIMAL_TYPE_NAME = "numeric";

    private Map<Class<?>, ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, ?>> converters = new HashMap<>();
    private ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Long> timestampConverter;
    private ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Long> timestamptzConverter;

    @Override
    public List<? extends Object> toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column tableColumn) throws ColumnConversionFailedException {
        List<?> elements = (List<?>) schemalessKafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(tableColumn);
        if (CollectionUtils.isEmpty(elements)) {
            return Collections.emptyList();
        }

        // jdbc driver is not creating timestamps but array[integers] since the values are coming as ints
        if (typeName.equals(INTEGER_TYPE_NAME)) {
            return asIntArray(elements, tableColumn);
        }
        if (typeName.equals(BIGINT_TYPE_NAME)) {
            return asLongArray(elements, tableColumn);
        }
        if (typeName.equals(TIMESTAMP_TYPE_NAME)) {
            return asTimestampArray(elements, tableColumn);
        }
        if (typeName.equals(TIMESTAMPTZ_TYPE_NAME)) {
            return asTimestamptzArray(elements, tableColumn);
        }
        if (typeName.equals(REAL_TYPE_NAME)) {
            return asFloatArray(elements, tableColumn);
        }
        if (typeName.equals(DOUBLE_TYPE_NAME)) {
            return asDoubleArray(elements, tableColumn);
        }
        if (typeName.equals(DECIMAL_TYPE_NAME)) {
            return asDecimalArray(elements, tableColumn);
        }

        log.warn("Could not resolve type name: {}", typeName);
        return Collections.emptyList();
    }

    @Override
    public Class<List<? extends Object>> getConvertedType() {
        @SuppressWarnings("unchecked")
        Class<List<? extends Object>> clazz = (Class) List.class;
        return clazz;
    }

    public <R> void addConverter(Class<R> type, ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, R> converter) {
        //make sure the converters match the same type
        if (converter.getConvertedType() != type) {
            throw new IllegalArgumentException("Cannot convert to " + type + "using " + converter.getClass());
        }

        converters.put(type, converter);
    }

    public void setTimestampConverter(ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Long> converter) {
        if (converter.getConvertedType() != Long.class) {
            throw new IllegalArgumentException("Timestamp converter must convert to Long (micros)");
        }
        this.timestampConverter = converter;
    }

    public void setTimestamptzConverter(ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Long> converter) {
        if (converter.getConvertedType() != Long.class) {
            throw new IllegalArgumentException("Timestamptz converter must convert to Long (micros)");
        }
        this.timestamptzConverter = converter;
    }

    // NOTE once this https://packboard.atlassian.net/browse/FIR-50959 we need to check the inner data type rather than array(integer)
    // as this is should be the inner table column not the outer one
    private List<? extends Object> asIntArray(List<?> elements, TableSchema.Column tableColumn) {
        List<Integer> integers = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Integer> converter =
                (ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Integer>) converters.get(Integer.class);

        for (Object element : elements) {
            if (element == null) {
                integers.add(null);
            } else {
                Integer convertedValue = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                integers.add(convertedValue);
            }
        }
        return integers;
    }

    private List<? extends Object> asLongArray(List<?> elements, TableSchema.Column tableColumn) {
        List<Long> longs = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Long> converter =
                (ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Long>) converters.get(Long.class);

        for (Object element : elements) {
            if (element == null) {
                longs.add(null);
            } else {
                Long convertedValue = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                longs.add(convertedValue);
            }
        }
        return longs;
    }

    private List<? extends Object> asTimestampArray(List<?> elements, TableSchema.Column tableColumn) {
        List<Long> timestamps = new ArrayList<>();
        for (Object element : elements) {
            if (element == null) {
                timestamps.add(null);
            } else {
                Long convertedValue = timestampConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                timestamps.add(convertedValue);
            }
        }
        return timestamps;
    }

    private List<? extends Object> asTimestamptzArray(List<?> elements, TableSchema.Column tableColumn){
            List<Long> timestamps = new ArrayList<>();
            for (Object element : elements) {
                if (element == null) {
                    timestamps.add(null);
                } else {
                    Long convertedValue = timestamptzConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                    timestamps.add(convertedValue);
                }
            }
            return timestamps;
    }

    private List<? extends Object> asFloatArray(List<?> elements, TableSchema.Column tableColumn) {
        List<Float> floats = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Float> converter =
                (ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Float>) converters.get(Float.class);

        for (Object element : elements) {
            if (element == null) {
                floats.add(null);
            } else {
                Float convertedValue = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                floats.add(convertedValue);
            }
        }
        return floats;
    }

    private List<? extends Object> asDecimalArray(List<?> elements, TableSchema.Column tableColumn) {
        List<ByteBuffer> decimals = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, ByteBuffer> converter =
                (ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, ByteBuffer>) converters.get(ByteBuffer.class);

        for (Object element : elements) {
            if (element == null) {
                decimals.add(null);
            } else {
                ByteBuffer convertedValue = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                decimals.add(convertedValue);
            }
        }
        return decimals;
    }

    private List<? extends Object> asDoubleArray(List<?> elements, TableSchema.Column tableColumn) {
        List<Double> doubles = new ArrayList<>();

        @SuppressWarnings("unchecked")
        ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Double> converter =
                (ColumnDataTypeConverter<SchemalessKafkaMessageColumnValue, Double>) converters.get(Double.class);

        for (Object element : elements) {
            if (element == null) {
                doubles.add(null);
            } else {
                Double convertedValue = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                doubles.add(convertedValue);
            }
        }
        return doubles;
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        // NOTE once this https://packboard.atlassian.net/browse/FIR-50959 we need to check the inner data type rather than array(integer)
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return INTEGER_TYPE_NAME;
        }
        if (fireboltColumn.getDataType().equals("array(bigint)")) {
            return BIGINT_TYPE_NAME;
        }
        if (fireboltColumn.getDataType().equals("array(timestamp)")) {
            return TIMESTAMP_TYPE_NAME;
        }
        if (fireboltColumn.getDataType().equals("array(timestamptz)")) {
            return TIMESTAMPTZ_TYPE_NAME;
        }
        if (fireboltColumn.getDataType().equals("array(real)")) {
            return REAL_TYPE_NAME;
        }
        if (fireboltColumn.getDataType().equals("array(double precision)")) {
            return DOUBLE_TYPE_NAME;
        }
        if (fireboltColumn.getDataType().equals("array(numeric)") || fireboltColumn.getDataType().equals("array(decimal)")) {
            return DECIMAL_TYPE_NAME;
        }

        // add more data types
        return "string";
    }

}


