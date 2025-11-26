package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractColumnTypeConverter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Converts a schemaless kafka message column value to a byte buffer for parquet/avro BYTES.
 * Accepts:
 *  - byte[]
 *  - ByteBuffer
 *  - String (encoded as UTF-8 bytes)
 */
public class SchemalessByteaColumnDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue, ByteBuffer> {

    @Override
    public ByteBuffer toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }

        if (value instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) value;
            // ensure independent buffer for writing
            ByteBuffer copy = ByteBuffer.allocate(buf.remaining());
            ByteBuffer dup = buf.slice();
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


