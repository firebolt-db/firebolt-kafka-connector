package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestampConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

/**
 * Schema-based adapter converting values to parquet/avro timestamp-micros (Long).
 */
public class SchemaTimestampBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue, Long> {

    @Override
    public Long toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemaKafkaMessageColumnValue.getValue();

        if (value instanceof Date) {
            Date date = (Date) schemaKafkaMessageColumnValue.getValue();
            return asMicros(date.toInstant());
        }

        if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String dateTimeAsString = (String) value;
            if (FireboltTimestampConverter.isIsoLocalDateTime(dateTimeAsString)) {
                LocalDateTime ldt = FireboltTimestampConverter.parseIsoLocalDateTime(dateTimeAsString);
                Instant instant = ldt.toInstant(ZoneOffset.UTC);
                return asMicros(instant);
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

    private long asMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
    }

    @Override
    public Class<Long> getConvertedType() {
        return Long.class;
    }
}


