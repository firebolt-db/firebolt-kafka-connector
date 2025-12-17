package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Shared bytea conversion for binary parquet ingestion. Produces Avro/Parquet BYTES as ByteBuffer.
 * Accepts byte[], ByteBuffer, and String (encoded as UTF-8).
 */
public abstract class AbstractByteaBinaryColumnDataTypeConverter<T extends SchemalessKafkaMessageColumnValue> extends AbstractBinaryColumnTypeConverter<T, ByteBuffer> {

    @Override
    public ByteBuffer toParquetValue(T kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = kafkaMessageColumnValue.getValue();

        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }

        if (value instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) value;
            ByteBuffer dup = buf.slice();
            ByteBuffer copy = ByteBuffer.allocate(dup.remaining());
            copy.put(dup);
            copy.flip();
            return copy;
        }

        if (value instanceof String) {
            return ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

    @Override
    public Class<ByteBuffer> getConvertedType() {
        return ByteBuffer.class;
    }
}


