package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Converts a schemaless kafka message column value to a timestamp-micros (Long) for parquet/avro
 */
public class SchemalessTimestampColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, Long> {

    @Override
    public Long toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof String) {
            String dateTimeAsString = (String) value;
            if (FireboltTimestampConverter.isIsoLocalDateTime(dateTimeAsString)) {
                LocalDateTime ldt = FireboltTimestampConverter.parseIsoLocalDateTime(dateTimeAsString);
                Instant instant = ldt.toInstant(ZoneOffset.UTC);
                long micros = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
                return micros;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "String value cannot be converted to a timestamp column in firebolt as it is not an ISO local ");
        }

        if (value instanceof Number) {
            long numeric = ((Number) value).longValue();
            // if already in micros (very large), pass through; otherwise convert millis -> micros
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


