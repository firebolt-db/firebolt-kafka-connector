package com.firebolt.kafka.connect.ingestion.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class UploadIngestionServiceTest {

    private static final String TOPIC = "events";

    private Connection connection;
    private FireboltParquetStatement statement;
    private ErrorReporter errorReporter;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        FireboltConnection fireboltConnection = mock(FireboltConnection.class);
        statement = mock(FireboltParquetStatement.class);
        when(connection.unwrap(FireboltConnection.class)).thenReturn(fireboltConnection);
        when(fireboltConnection.createParquetStatement()).thenReturn(statement);
    }

    private UploadIngestionService service(TableSchema tableSchema, boolean errorToleranceAll) {
        errorReporter = mock(ErrorReporter.class);
        return new UploadIngestionService(connection, errorReporter, errorToleranceAll, tableSchema);
    }

    private SinkRecord record(Schema valueSchema, Object value, long offset) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, offset);
    }

    // ---- schema-carrying records -> Parquet / read_parquet ----

    @Test
    void schemaRecordsRoundTripThroughParquet() throws Exception {
        Schema addressSchema = SchemaBuilder.struct().name("Address")
                .field("city", Schema.STRING_SCHEMA).build();
        Schema valueSchema = SchemaBuilder.struct().name("Event")
                .field("id", Schema.INT64_SCHEMA)
                .field("amount", Decimal.schema(2))
                .field("created_at", Timestamp.SCHEMA)
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("address", addressSchema)
                .build();
        Struct value = new Struct(valueSchema)
                .put("id", 7L)
                .put("amount", new BigDecimal("12.34"))
                .put("created_at", new java.util.Date(1718000000000L))
                .put("tags", List.of("a", "b"))
                .put("address", new Struct(addressSchema).put("city", "tlv"));

        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        tableSchema.addColumn("amount", "numeric(38,2)", Types.NUMERIC, true);
        tableSchema.addColumn("created_at", "timestamp", Types.TIMESTAMP, true);
        tableSchema.addColumn("tags", "array(text)", Types.ARRAY, true);
        tableSchema.addColumn("address", "text", Types.VARCHAR, true);

        service(tableSchema, false).addRecords(List.of(record(valueSchema, value, 1L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"id\", \"amount\", \"created_at\", \"tags\", \"address\") "
                + "SELECT CAST(\"id\" AS bigint), CAST(\"amount\" AS numeric(38,2)), CAST(\"created_at\" AS timestamp), "
                + "CAST(\"tags\" AS array(text)), CAST(\"address\" AS text) "
                + "FROM read_parquet('upload://batch')", upload.sql);
        List<GenericRecord> rows = readParquet(upload.payload);
        assertEquals(1, rows.size());
        assertEquals(7L, rows.get(0).get("id"));
        assertEquals(java.time.Instant.ofEpochMilli(1718000000000L), rows.get(0).get("created_at"));
    }

    @Test
    void schemaRecordsSplitParquetFilePerSchema() throws Exception {
        Schema v1 = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();
        Schema v2 = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA)
                .field("b", Schema.OPTIONAL_STRING_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("a", "bigint", Types.BIGINT, false);
        tableSchema.addColumn("b", "text", Types.VARCHAR, true);

        service(tableSchema, false).addRecords(List.of(
                record(v1, new Struct(v1).put("a", 1L), 0L),
                record(v2, new Struct(v2).put("a", 2L).put("b", "x"), 1L),
                record(v1, new Struct(v1).put("a", 3L), 2L)));

        verify(statement, times(2)).execute(anyString(), anyMap());
    }

    // ---- schemaless JSON records -> NDJSON / read_json ----

    @Test
    void schemalessRecordsIngestAsNdjsonViaReadJson() throws Exception {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("id", 1);
        first.put("name", "alice");
        first.put("nested", Map.of("k", "v"));
        Map<String, Object> second = new LinkedHashMap<>();
        second.put("id", 2);
        second.put("name", "bob");

        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        tableSchema.addColumn("name", "text", Types.VARCHAR, true);
        tableSchema.addColumn("nested", "struct(k text)", Types.STRUCT, true);

        service(tableSchema, false).addRecords(List.of(record(null, first, 0L), record(null, second, 1L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"id\", \"name\", \"nested\") "
                + "SELECT CAST(\"id\" AS bigint), CAST(\"name\" AS text), CAST(\"nested\" AS struct(k text)) "
                + "FROM read_json('upload://batch')", upload.sql);
        // payload is newline-delimited JSON, one object per record
        String[] lines = new String(upload.payload, StandardCharsets.UTF_8).split("\n");
        assertEquals(2, lines.length);
        assertTrue(lines[0].contains("\"id\":1") && lines[0].contains("\"name\":\"alice\""));
        assertTrue(lines[0].contains("\"nested\":{\"k\":\"v\"}"));
        assertTrue(lines[1].contains("\"id\":2"));
    }

    @Test
    void loneJsonColumnStoresWholeDocumentViaParseAsJson() throws Exception {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("k", "v");
        value.put("n", 3);

        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("doc", "JSON", Types.OTHER, true);

        service(tableSchema, false).addRecords(List.of(record(null, value, 0L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"doc\") SELECT * FROM read_json('upload://batch', PARSE_AS_JSON => TRUE)", upload.sql);
        assertTrue(new String(upload.payload, StandardCharsets.UTF_8).contains("\"k\":\"v\""));
    }

    @Test
    void schemalessProjectionDropsUnknownFieldsAndMatchesCaseInsensitively() throws Exception {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("UserId", 1);
        value.put("extra", "dropped");

        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("userid", "bigint", Types.BIGINT, false);

        service(tableSchema, false).addRecords(List.of(record(null, value, 0L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"userid\") SELECT CAST(\"UserId\" AS bigint) FROM read_json('upload://batch')", upload.sql);
    }

    // ---- shared behavior ----

    @Test
    void mixedSchemaAndSchemalessBatchUploadsBothFlows() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);

        service(tableSchema, false).addRecords(List.of(
                record(valueSchema, new Struct(valueSchema).put("id", 1L), 0L),
                record(null, Map.of("id", 2), 1L)));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(statement, times(2)).execute(sql.capture(), anyMap());
        assertTrue(sql.getAllValues().stream().anyMatch(s -> s.contains("read_parquet")));
        assertTrue(sql.getAllValues().stream().anyMatch(s -> s.contains("read_json")));
    }

    @Test
    void appendsLiteralColumnsWhenTableHasThem() throws Exception {
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        tableSchema.addColumn("batch_id", "text", Types.VARCHAR, true);

        service(tableSchema, false).addRecords(
                List.of(record(null, Map.of("id", 1), 0L)),
                Map.of("batch_id", "my-batch", "not_a_column", "ignored"));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"id\", \"batch_id\") SELECT CAST(\"id\" AS bigint), CAST('my-batch' AS text) FROM read_json('upload://batch')", upload.sql);
    }

    @Test
    void skipsTombstonesAndUploadsNothingForEmptyBatch() throws Exception {
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        service(tableSchema, false).addRecords(List.of(record(null, null, 0L)));
        verify(statement, never()).execute(anyString(), anyMap());
    }

    @Test
    void failsWhenNoFieldsMatchAnyColumn() {
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("other", "bigint", Types.BIGINT, false);
        assertThrows(SQLException.class, () -> service(tableSchema, false)
                .addRecords(List.of(record(null, Map.of("id", 1), 0L))));
    }

    @Test
    void reportsBadRecordsToDlqWhenTolerant() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);

        // schema'd record whose value isn't a Struct, and a schemaless value that isn't a Map
        service(tableSchema, true).addRecords(List.of(
                record(valueSchema, "not a struct", 0L),
                record(null, "not a map", 1L),
                record(null, Map.of("id", 9), 2L)));

        verify(errorReporter, times(2)).report(any(SinkRecord.class), any(Exception.class));
        // the one good record still ingests
        verify(statement, times(1)).execute(anyString(), anyMap());
    }

    @Test
    void throwsOnBadRecordWhenNotTolerant() {
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        assertThrows(RecordConversionException.class, () -> service(tableSchema, false)
                .addRecords(List.of(record(null, "not a map", 0L))));
        verify(errorReporter, never()).report(any(SinkRecord.class), any(Exception.class));
    }

    // ---- helpers ----

    private static final class Upload {
        final String sql;
        final byte[] payload;

        Upload(String sql, byte[] payload) {
            this.sql = sql;
            this.payload = payload;
        }
    }

    private Upload captureSingleUpload() throws SQLException {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, byte[]>> filesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(statement).execute(sqlCaptor.capture(), filesCaptor.capture());
        return new Upload(sqlCaptor.getValue(), filesCaptor.getValue().get("batch"));
    }

    private List<GenericRecord> readParquet(byte[] bytes) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new ByteArrayInputFile(bytes)).build()) {
            for (GenericRecord row = reader.read(); row != null; row = reader.read()) {
                rows.add(row);
            }
        }
        return rows;
    }

    private static final class ByteArrayInputFile implements InputFile {
        private final byte[] bytes;

        ByteArrayInputFile(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long getLength() {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
            ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
            return new DelegatingSeekableInputStream(stream) {
                @Override
                public long getPos() {
                    return bytes.length - stream.available();
                }

                @Override
                public void seek(long newPos) throws IOException {
                    stream.reset();
                    if (stream.skip(newPos) != newPos) {
                        throw new IOException("Could not seek to " + newPos);
                    }
                }
            };
        }
    }
}
