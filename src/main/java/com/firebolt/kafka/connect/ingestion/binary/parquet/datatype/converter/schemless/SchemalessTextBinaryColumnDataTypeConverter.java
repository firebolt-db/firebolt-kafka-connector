package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.util.Map;

/**
 * Converts a schemaless kafka message column value to a text (String) for parquet/avro
 */
public class SchemalessTextBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue, String> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public String toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

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


