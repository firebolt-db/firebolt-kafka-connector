package com.firebolt.kafka.connect.ingestion.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
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

    private UploadIngestionService service(boolean errorToleranceAll) {
        errorReporter = mock(ErrorReporter.class);
        return new UploadIngestionService(connection, errorReporter, errorToleranceAll, "t");
    }

    private SinkRecord record(Schema valueSchema, Object value, long offset) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, offset);
    }

    // ---- schema-carrying records -> Avro / read_avro ----

    @Test
    void schemaRecordsRoundTripThroughAvro() throws Exception {
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

        service(false).addRecords(List.of(record(valueSchema, value, 1L)));

        Upload upload = captureSingleUpload();
        // columns come from the record's own fields, quoted on both sides (matched to the table
        // column whose name equals the field exactly).
        assertEquals("INSERT INTO \"t\" (\"id\", \"amount\", \"created_at\", \"tags\", \"address\") "
                + "SELECT \"id\", \"amount\", \"created_at\", \"tags\", \"address\" "
                + "FROM read_avro('upload://batch')", upload.sql);
        List<GenericRecord> rows = readAvro(upload.payload);
        assertEquals(1, rows.size());
        assertEquals(7L, rows.get(0).get("id"));
        // AvroData maps Connect Timestamp -> avro long with logicalType timestamp-millis.
        assertEquals(1718000000000L, rows.get(0).get("created_at"));
    }

    @Test
    void schemaRecordsSplitAvroFilePerSchema() throws Exception {
        Schema v1 = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();
        Schema v2 = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA)
                .field("b", Schema.OPTIONAL_STRING_SCHEMA).build();

        service(false).addRecords(List.of(
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

        service(false).addRecords(List.of(record(null, first, 0L), record(null, second, 1L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"id\", \"name\", \"nested\") "
                + "SELECT \"id\", \"name\", \"nested\" FROM read_json('upload://batch')", upload.sql);
        // payload is newline-delimited JSON, one object per record
        String[] lines = new String(upload.payload, StandardCharsets.UTF_8).split("\n");
        assertEquals(2, lines.length);
        assertTrue(lines[0].contains("\"id\":1") && lines[0].contains("\"name\":\"alice\""));
        assertTrue(lines[0].contains("\"nested\":{\"k\":\"v\"}"));
        assertTrue(lines[1].contains("\"id\":2"));
    }

    @Test
    void projectsEveryRecordFieldByItsOwnName() throws Exception {
        // No table lookup: all fields are projected. The table is the contract — a field that is not
        // a column makes Firebolt reject the batch (not exercised here; the statement is mocked).
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("UserId", 1);
        value.put("extra", "x");

        service(false).addRecords(List.of(record(null, value, 0L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"UserId\", \"extra\") SELECT \"UserId\", \"extra\" FROM read_json('upload://batch')", upload.sql);
    }


    // ---- shared behavior ----

    @Test
    void mixedSchemaAndSchemalessBatchUploadsBothFlows() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();

        service(false).addRecords(List.of(
                record(valueSchema, new Struct(valueSchema).put("id", 1L), 0L),
                record(null, Map.of("id", 2), 1L)));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(statement, times(2)).execute(sql.capture(), anyMap());
        assertTrue(sql.getAllValues().stream().anyMatch(s -> s.contains("read_avro")));
        assertTrue(sql.getAllValues().stream().anyMatch(s -> s.contains("read_json")));
    }

    @Test
    void appendsLiteralColumns() throws Exception {
        service(false).addRecords(
                List.of(record(null, Map.of("id", 1), 0L)),
                Map.of("batch_id", "my-batch"));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"id\", \"batch_id\") SELECT \"id\", 'my-batch' FROM read_json('upload://batch')", upload.sql);
    }

    @Test
    void skipsTombstonesAndUploadsNothingForEmptyBatch() throws Exception {
        service(false).addRecords(List.of(record(null, null, 0L)));
        verify(statement, never()).execute(anyString(), anyMap());
    }

    @Test
    void emptyJsonObjectsProduceNoUpload() throws Exception {
        service(false).addRecords(List.of(record(null, Map.of(), 0L)));
        verify(statement, never()).execute(anyString(), anyMap());
    }

    @Test
    void literalColumnCollidingWithFieldIsNotDuplicated() throws Exception {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", 1);
        value.put("batch_id", "fromRecord");

        service(false).addRecords(List.of(record(null, value, 0L)), Map.of("batch_id", "generated"));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"id\", \"batch_id\") SELECT \"id\", \"batch_id\" FROM read_json('upload://batch')", upload.sql);
    }

    @Test
    void multiGroupBatchRunsInOneTransaction() throws Exception {
        when(connection.getAutoCommit()).thenReturn(true);
        Schema vs = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();

        service(false).addRecords(List.of(
                record(vs, new Struct(vs).put("id", 1L), 0L),
                record(null, Map.of("id", 2), 1L)));

        verify(connection).setAutoCommit(false);
        verify(statement, times(2)).execute(anyString(), anyMap());
        verify(connection).commit();
        verify(connection).setAutoCommit(true);
    }

    @Test
    void isolatesPoisonRecordViaSplitRetryWhenTolerant() throws Exception {
        // full batch [0,2) fails; [0,1) succeeds; [1,2) (one record) fails -> DLQ that record.
        when(statement.execute(anyString(), anyMap()))
                .thenThrow(new SQLException("batch rejected"))
                .thenReturn(true)
                .thenThrow(new SQLException("poison record"));

        service(true).addRecords(List.of(
                record(null, Map.of("id", 1), 0L),
                record(null, Map.of("id", 2), 1L)));

        verify(statement, times(3)).execute(anyString(), anyMap());
        verify(errorReporter, times(1)).report(any(SinkRecord.class), any(Exception.class));
    }

    @Test
    void doesNotSplitOrDlqWhenNotTolerant() throws Exception {
        when(statement.execute(anyString(), anyMap())).thenThrow(new SQLException("boom"));

        org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> service(false).addRecords(List.of(
                record(null, Map.of("id", 1), 0L),
                record(null, Map.of("id", 2), 1L))));

        verify(statement, times(1)).execute(anyString(), anyMap());
        verify(errorReporter, never()).report(any(SinkRecord.class), any(Exception.class));
    }

    @Test
    void multiGroupBatchRollsBackOnFailure() throws Exception {
        when(connection.getAutoCommit()).thenReturn(true);
        when(statement.execute(anyString(), anyMap())).thenReturn(true).thenThrow(new SQLException("boom"));
        Schema vs = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();

        org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> service(false).addRecords(List.of(
                record(vs, new Struct(vs).put("id", 1L), 0L),
                record(null, Map.of("id", 2), 1L))));

        verify(connection).rollback();
        verify(connection, never()).commit();
    }

    @Test
    void reportsBadRecordsToDlqWhenTolerant() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();

        // schema'd record whose value isn't a Struct, and a schemaless value that isn't a Map
        service(true).addRecords(List.of(
                record(valueSchema, "not a struct", 0L),
                record(null, "not a map", 1L),
                record(null, Map.of("id", 9), 2L)));

        verify(errorReporter, times(2)).report(any(SinkRecord.class), any(Exception.class));
        verify(statement, times(1)).execute(anyString(), anyMap());
    }

    @Test
    void throwsOnBadRecordWhenNotTolerant() {
        org.junit.jupiter.api.Assertions.assertThrows(RecordConversionException.class, () -> service(false)
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

    private List<GenericRecord> readAvro(byte[] bytes) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> reader =
                     new DataFileReader<>(new SeekableByteArrayInput(bytes), datumReader)) {
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        }
        return rows;
    }
}
