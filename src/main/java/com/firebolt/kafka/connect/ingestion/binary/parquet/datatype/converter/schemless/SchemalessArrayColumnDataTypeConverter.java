package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.CompositeDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltByteaConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Slf4j
public class SchemalessArrayColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, List<? extends Object>> {

    private static final String DATE_ARRAY_TYPE_NAME = "date";
    private static final String TIMESTAMP_ARRAY_TYPE_NAME = "timestamp";
    private static final String TIMESTAMPTZ_ARRAY_TYPE_NAME = "timestamptz";
    private static final String BYTEA_ARRAY_TYPE_NAME = "bytea";
    private static final String INTEGER_TYPE_NAME = "integer";

    @Override
    public List<? extends Object> toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column tableColumn) throws ColumnConversionFailedException {
        List<Object> elements = (List) schemalessKafkaMessageColumnValue.getValue();

        String typeName = detectTypeName(tableColumn);
        if (CollectionUtils.isEmpty(elements)) {
            return Collections.emptyList();
        }

        // jdbc driver is not creating timestamps but array[integers] since the values are coming as ints
        if (typeName.equals(INTEGER_TYPE_NAME)) {
            return asIntArray(elements, tableColumn);
        }

        log.warn("Could not resolve type name: {}", typeName);
        return Collections.emptyList();
    }

    private List<? extends Object> asIntArray(List<Object> elements, TableSchema.Column tableColumn) {
        List<Integer> integers = new ArrayList<>();
        for (Object element : elements) {
            integers.add(asInteger(element, tableColumn));
        }
        return integers;
    }

    private Integer asInteger(Object value, TableSchema.Column tableColumn) {
        if (value == null) {
            return null;
        }

        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).intValue();
        }

        // check if the long value can "fit" into an int value without data loss
        if (value instanceof Long) {
            Long longValue = (Long) value;
            if (longValue >= Integer.MIN_VALUE && longValue<= Integer.MAX_VALUE) {
                return longValue.intValue();
            } else {
                throw new ColumnConversionFailedException(tableColumn.getName(), tableColumn.getDataType(), "You are trying to set a long value into an int value column. That would result in data loss.");
            }
        }

        if (value instanceof String) {
            String str = (String) value;
            try {
                int parsed = Integer.parseInt(str.trim());
                return parsed;
            } catch (NumberFormatException e) {
                throw new ColumnConversionFailedException(tableColumn.getName(), tableColumn.getDataType(), "Cannot convert kafka message attribute to a integer due to NumberFormatException: " + e.getMessage());
            }
        }

        throw aColumnConversionFailedException(tableColumn, value);
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return "integer";
        } else if (fireboltColumn.getDataType().equals("array(timestamp)")) {
            return TIMESTAMP_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(timestamptz)")) {
            return TIMESTAMPTZ_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(date)")) {
            return DATE_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(numeric)")) {
            return "numeric";
        } else if (fireboltColumn.getDataType().equals("array(bigint)")) {
            return "bigint";
        } else if (fireboltColumn.getDataType().equals("array(real)")) {
            return "real";
        } else if (fireboltColumn.getDataType().equals("array(double)")) {
            return "double";
        } else if (fireboltColumn.getDataType().equals("array(text)")) {
            return "string";
        } else if (fireboltColumn.getDataType().equals("array(bytea)")) {
            return BYTEA_ARRAY_TYPE_NAME;
        } else if (fireboltColumn.getDataType().equals("array(boolean)")) {
            return "boolean";
        }

        // add more data types
        return "string";
    }

}


