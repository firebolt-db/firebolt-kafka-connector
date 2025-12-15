package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * Converts a schemaless kafka message column value to a timestamptz represented as timestamp-micros (Long) for parquet/avro.
 */
public class SchemalessTimestamptzBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue, Long> {

    @Override
    public Long toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof String) {
            String text = (String) value;
            if (FireboltTimestamptzConverter.isValidTimestamptz(text)) {
                OffsetDateTime odt = FireboltTimestamptzConverter.parseTimestamptz(text).withOffsetSameInstant(java.time.ZoneOffset.UTC);
                Instant instant = odt.toInstant();
                long seconds = instant.getEpochSecond();
                int nanos = instant.getNano();
                long micros = seconds * 1_000_000L + (nanos / 1_000);
                
                //NOTE the avro schema does not support nanoseconds with logial type. So we need to round it up ourselves.
                // This is not a problem for parquet, as it supports nanosecond precision.
                // Round micros based on sub-micro nanos: e.g., ...123456 -> 123; ...123678 -> 124
                if ((nanos % 1_000) >= 500) {
                    micros += 1;
                }
                return micros;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "String value cannot be converted to a timestamptz column in firebolt as it is not a valid ISO timestamptz");
        }

        if (value instanceof Number) {
            long numeric = ((Number) value).longValue();
            // If already micros, pass through; otherwise treat as millis and convert to micros
            if (numeric > 10_000_000_000_000L) {
                return numeric;
            } else {
                return numeric * 1_000L;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

    @Override
    public Class<Long> getConvertedType() {
        return Long.class;
    }
}


