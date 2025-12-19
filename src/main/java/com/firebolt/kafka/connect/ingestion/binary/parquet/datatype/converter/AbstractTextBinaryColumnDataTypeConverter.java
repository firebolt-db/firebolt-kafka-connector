package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.util.Map;

/**
 * Shared text conversion for binary parquet ingestion. Serializes Maps to JSON, otherwise String.valueOf.
 */
public abstract class AbstractTextBinaryColumnDataTypeConverter<T extends SchemalessKafkaMessageColumnValue> extends AbstractBinaryColumnTypeConverter<T, String> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public String toParquetValue(T kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value instanceof Map) {
            try {
                return OBJECT_MAPPER.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                        "Failed to serialize the message as json: " + e.getMessage());
            }
        }

        return String.valueOf(value);
    }

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }
}


