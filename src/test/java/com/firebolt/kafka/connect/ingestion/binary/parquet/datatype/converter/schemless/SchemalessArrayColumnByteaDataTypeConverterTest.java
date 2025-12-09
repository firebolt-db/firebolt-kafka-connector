package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemalessArrayColumnByteaDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessByteaColumnDataTypeConverter byteaConverter = new SchemalessByteaColumnDataTypeConverter();
    private final TableSchema.Column arrayCol = new TableSchema.Column("bytes", "array(bytea)", Types.ARRAY, false);

    @Test
    void convertsMixedElements() {
        arrayConverter.addConverter(FireboltColumnDataType.BYTEA, byteaConverter);
        List<Object> input = Arrays.asList(
                "hi",
                "a".getBytes(StandardCharsets.UTF_8),
                ByteBuffer.wrap("Z".getBytes(StandardCharsets.UTF_8)),
                null
        );
        List<?> out = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayCol);
        assertEquals(4, out.size());
        assertArrayEquals("hi".getBytes(StandardCharsets.UTF_8), getBytes((ByteBuffer) out.get(0)));
        assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), getBytes((ByteBuffer) out.get(1)));
        assertArrayEquals("Z".getBytes(StandardCharsets.UTF_8), getBytes((ByteBuffer) out.get(2)));
        assertEquals(null, out.get(3));
    }

    private byte[] getBytes(ByteBuffer buf) {
        byte[] copy = new byte[buf.remaining()];
        buf.get(copy);
        return copy;
    }
}


