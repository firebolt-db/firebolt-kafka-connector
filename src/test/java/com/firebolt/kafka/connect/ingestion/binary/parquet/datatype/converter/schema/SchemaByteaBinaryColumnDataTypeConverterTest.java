package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class SchemaByteaBinaryColumnDataTypeConverterTest {

    private final SchemaByteaBinaryColumnDataTypeConverter converter = new SchemaByteaBinaryColumnDataTypeConverter();
    private final TableSchema.Column col = new TableSchema.Column("b", "bytea", Types.BINARY, false);

    @Test
    void convertsByteArray() {
        byte[] data = "abc".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.BYTES)
                .value(data)
                .build(), col);
        assertArrayEquals(data, toArray(buf));
    }

    @Test
    void convertsByteBuffer() {
        byte[] data = "xyz".getBytes(StandardCharsets.UTF_8);
        ByteBuffer input = ByteBuffer.wrap(data);
        ByteBuffer buf = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.BYTES)
                .value(input)
                .build(), col);
        assertArrayEquals(data, toArray(buf));
    }

    @Test
    void convertsStringUtf8() {
        String s = "hello";
        ByteBuffer buf = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(s)
                .build(), col);
        assertArrayEquals(s.getBytes(StandardCharsets.UTF_8), toArray(buf));
    }

    private static byte[] toArray(ByteBuffer buf) {
        ByteBuffer dup = buf.slice();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }
}


