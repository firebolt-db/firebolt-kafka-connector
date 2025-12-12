package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessByteaBinaryColumnDataTypeConverterTest {

    private final SchemalessByteaBinaryColumnDataTypeConverter converter = new SchemalessByteaBinaryColumnDataTypeConverter();
    private final TableSchema.Column byteaCol = new TableSchema.Column("b", "bytea", Types.BINARY, false);

    @Test
    void acceptsByteArray() {
        byte[] src = new byte[]{1, 2, 3};
        ByteBuffer out = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(src), byteaCol);
        byte[] copy = new byte[out.remaining()];
        out.get(copy);
        assertArrayEquals(src, copy);
    }

    @Test
    void acceptsByteBuffer() {
        byte[] src = "abc".getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.wrap(src);
        ByteBuffer out = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(bb), byteaCol);
        byte[] copy = new byte[out.remaining()];
        out.get(copy);
        assertArrayEquals(src, copy);
    }

    @Test
    void acceptsStringUtf8() {
        String s = "hello";
        ByteBuffer out = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(s), byteaCol);
        byte[] copy = new byte[out.remaining()];
        out.get(copy);
        assertArrayEquals(s.getBytes(StandardCharsets.UTF_8), copy);
    }

    @Test
    void invalidTypeThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue(12.3), byteaCol));
    }
}


