package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.CompositeDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltByteaConverter;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.ColumnDataTypeConverter;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

@Slf4j
public class SchemalessArrayColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, List<? extends Object>> {

    private static final String INTEGER_TYPE_NAME = "integer";

    private Map<Class, ColumnDataTypeConverter> converters = new HashMap<>();

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

    @Override
    public Class<List<? extends Object>> getConvertedType() {
        @SuppressWarnings("unchecked")
        Class<List<? extends Object>> clazz = (Class) List.class;
        return clazz;
    }

    public void addConverter(Class type, ColumnDataTypeConverter converter) {
        //make sure the converters match the same type
        if (converter.getConvertedType() != type) {
            throw new IllegalArgumentException("Cannot convert to " + type + "using " + converter.getClass());
        }

        converters.put(type, converter);
    }

    // NOTE once this https://packboard.atlassian.net/browse/FIR-50959 we need to check the inner data type rather than array(integer)
    // as this is should be the inner table column not the outer one
    private List<? extends Object> asIntArray(List<Object> elements, TableSchema.Column tableColumn) {
        List<Integer> integers = new ArrayList<>();
        for (Object element : elements) {
            if (element == null) {
                integers.add(null);
            } else {
                ColumnDataTypeConverter converter = converters.get(Integer.class);
                Integer convertedValue = (Integer) converter.toParquetValue(new SchemalessKafkaMessageColumnValue(element), tableColumn);
                integers.add(convertedValue);
            }
        }
        return integers;
    }

    private String detectTypeName(TableSchema.Column fireboltColumn) {
        // NOTE once this https://packboard.atlassian.net/browse/FIR-50959 we need to check the inner data type rather than array(integer)
        if (fireboltColumn.getDataType().equals("array(integer)")) {
            return INTEGER_TYPE_NAME;
        }

        // add more data types
        return "string";
    }

}


