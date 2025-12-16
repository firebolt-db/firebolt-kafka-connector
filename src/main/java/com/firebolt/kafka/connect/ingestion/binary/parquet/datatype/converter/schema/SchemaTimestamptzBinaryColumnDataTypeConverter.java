package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.FireboltTimestamptzConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

/**
 * Schema-based adapter converting values to parquet/avro timestamp-micros (Long) for timestamptz.
 */
public class SchemaTimestamptzBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue, Long> {

    @Override
    public Long toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemaKafkaMessageColumnValue.getValue();

        if (value instanceof Date) {
            Date date = (Date) value;
            return asMicros(date.toInstant());
        }

        if (schemaKafkaMessageColumnValue.getSchemaType() == Schema.Type.STRING) {
            String text = (String) value;
            if (FireboltTimestamptzConverter.isValidTimestamptz(text)) {
                OffsetDateTime odt = FireboltTimestamptzConverter.parseTimestamptz(text).withOffsetSameInstant(java.time.ZoneOffset.UTC);
                Instant instant = odt.toInstant();
                long seconds = instant.getEpochSecond();
                int nanos = instant.getNano();
                long micros = seconds * 1_000_000L + (nanos / 1_000);
                // Round to the nearest micro based on remaining nanos (Avro logical type is micros-only)
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

    private long asMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
    }

    @Override
    public Class<Long> getConvertedType() {
        return Long.class;
    }
}


