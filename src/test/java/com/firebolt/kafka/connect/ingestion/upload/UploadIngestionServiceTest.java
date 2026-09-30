package com.firebolt.kafka.connect.ingestion.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
        return service(errorToleranceAll, false);
    }

    private UploadIngestionService service(boolean errorToleranceAll, boolean jsonConsolidateUploads) {
        errorReporter = mock(ErrorReporter.class);
        return new UploadIngestionService(connection, errorReporter, errorToleranceAll, "t", jsonConsolidateUploads);
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
        // no column list: Firebolt matches the file's fields to the table's columns by name.
        assertEquals("INSERT INTO \"t\" BY NAME SELECT * FROM read_avro('upload://batch')", upload.sql);
        List<GenericRecord> rows = readAvro(upload.payload);
        assertEquals(1, rows.size());
        assertEquals(7L, rows.get(0).get("id"));
        // AvroData maps Connect Timestamp -> avro long with logicalType timestamp-millis.
        assertEquals(1718000000000L, rows.get(0).get("created_at"));
        // The "amount" field is a Connect Decimal with no declared precision; rather than AvroData's
        // default of 64 (which the engine rejects), the connector defaults it to Firebolt's NUMERIC(38).
        org.apache.avro.LogicalTypes.Decimal amountType = (org.apache.avro.LogicalTypes.Decimal)
                org.apache.avro.LogicalTypes.fromSchema(rows.get(0).getSchema().getField("amount").schema());
        assertEquals(38, amountType.getPrecision());
        assertEquals(2, amountType.getScale());
    }

    @Test
    void capsDeclaredDecimalPrecisionAt38() throws Exception {
        // e.g. a Postgres NUMERIC(50,2) via Debezium: values that fit NUMERIC(38,2) land; larger ones
        // are rejected by the engine on read (the unscaled bytes are untouched).
        Schema valueSchema = SchemaBuilder.struct().name("Event")
                .field("amount", Decimal.builder(2).parameter("connect.decimal.precision", "50").build())
                .build();

        service(false).addRecords(List.of(record(valueSchema,
                new Struct(valueSchema).put("amount", new BigDecimal("1.50")), 0L)));

        GenericRecord row = readAvro(captureSingleUpload().payload).get(0);
        org.apache.avro.LogicalTypes.Decimal amountType = (org.apache.avro.LogicalTypes.Decimal)
                org.apache.avro.LogicalTypes.fromSchema(row.getSchema().getField("amount").schema());
        assertEquals(38, amountType.getPrecision());
        assertEquals(2, amountType.getScale());
    }

    @Test
    void optionalStructValueSchemaIsWrittenAsItsRecordBranch() throws Exception {
        // AvroData maps an optional struct to a [null, record] union; the file needs the record schema.
        Schema valueSchema = SchemaBuilder.struct().name("Event").optional().field("id", Schema.INT64_SCHEMA).build();

        service(false).addRecords(List.of(record(valueSchema, new Struct(valueSchema).put("id", 5L), 0L)));

        GenericRecord row = readAvro(captureSingleUpload().payload).get(0);
        assertEquals(org.apache.avro.Schema.Type.RECORD, row.getSchema().getType());
        assertEquals(5L, row.get("id"));
    }

    @Test
    void capsDecimalPrecisionInsideNestedStructsAndArrays() throws Exception {
        Schema decimal = Decimal.schema(2); // no precision -> AvroData would declare 64
        Schema line = SchemaBuilder.struct().name("Line").field("price", decimal).build();
        Schema valueSchema = SchemaBuilder.struct().name("Order")
                .field("lines", SchemaBuilder.array(line).build())
                .build();
        Struct value = new Struct(valueSchema)
                .put("lines", List.of(new Struct(line).put("price", new BigDecimal("9.99"))));

        service(false).addRecords(List.of(record(valueSchema, value, 0L)));

        org.apache.avro.Schema price = readAvro(captureSingleUpload().payload).get(0).getSchema()
                .getField("lines").schema().getElementType().getField("price").schema();
        assertEquals(38, ((org.apache.avro.LogicalTypes.Decimal) org.apache.avro.LogicalTypes.fromSchema(price)).getPrecision());
    }

    @Test
    void nonStructValueSchemaIsABadRecord() throws Exception {
        service(true).addRecords(List.of(record(Schema.STRING_SCHEMA, "plain string", 0L)));

        verify(statement, never()).execute(anyString(), anyMap());
        verify(errorReporter).report(any(SinkRecord.class), any(RecordConversionException.class));
        org.junit.jupiter.api.Assertions.assertThrows(RecordConversionException.class,
                () -> service(false).addRecords(List.of(record(Schema.STRING_SCHEMA, "plain string", 0L))));
    }

    @Test
    void recordThatAvroDataCannotConvertIsABadRecordAndTheRestLand() throws Exception {
        // AvroData requires a Decimal value's scale to equal the schema's scale.
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("amount", Decimal.schema(2)).build();

        service(true).addRecords(List.of(
                record(valueSchema, new Struct(valueSchema).put("amount", new BigDecimal("1.234")), 0L),
                record(valueSchema, new Struct(valueSchema).put("amount", new BigDecimal("1.23")), 1L)));

        verify(errorReporter).report(any(SinkRecord.class), any(RecordConversionException.class));
        assertEquals(1, readAvro(captureSingleUpload().payload).size());
    }

    @Test
    void multiGroupBatchDefersToAnOuterTransaction() throws Exception {
        // The post-processing decorator turns auto-commit off and owns commit/rollback.
        when(connection.getAutoCommit()).thenReturn(false);
        Schema vs = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();

        service(false).addRecords(List.of(
                record(vs, new Struct(vs).put("id", 1L), 0L),
                record(null, Map.of("id", 2), 1L)));

        verify(statement, times(2)).execute(anyString(), anyMap());
        verify(connection, never()).setAutoCommit(anyBoolean());
        verify(connection, never()).commit();
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
        second.put("nested", null);

        service(false).addRecords(List.of(record(null, first, 0L), record(null, second, 1L)));

        Upload upload = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" BY NAME SELECT * FROM read_json('upload://batch')", upload.sql);
        // payload is newline-delimited JSON, one object per record
        String[] lines = new String(upload.payload, StandardCharsets.UTF_8).split("\n");
        assertEquals(2, lines.length);
        assertTrue(lines[0].contains("\"id\":1") && lines[0].contains("\"name\":\"alice\""));
        assertTrue(lines[0].contains("\"nested\":{\"k\":\"v\"}"));
        assertTrue(lines[1].contains("\"id\":2"));
    }

    @Test
    void schemalessRecordsUploadOnePayloadPerKeySet() throws Exception {
        // read_json infers one schema per upload: a key absent from one record but present in another
        // would surface as NULL for the former, overriding the column DEFAULT. So each distinct key set
        // (order-insensitive) is its own upload.
        Map<String, Object> idOnly = Map.of("id", 1);
        Map<String, Object> withScore = new LinkedHashMap<>();
        withScore.put("id", 2);
        withScore.put("score", 5);
        Map<String, Object> withScoreReordered = new LinkedHashMap<>();
        withScoreReordered.put("score", 6);
        withScoreReordered.put("id", 3);

        service(false).addRecords(List.of(
                record(null, idOnly, 0L), record(null, withScore, 1L),
                record(null, withScoreReordered, 2L), record(null, Map.of("id", 4), 3L)));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, byte[]>> files = ArgumentCaptor.forClass(Map.class);
        verify(statement, times(2)).execute(anyString(), files.capture());
        List<String> payloads = new ArrayList<>();
        files.getAllValues().forEach(f -> payloads.add(new String(f.get("batch"), StandardCharsets.UTF_8)));
        assertEquals("{\"id\":1}\n{\"id\":4}\n", payloads.get(0));
        assertEquals(2, payloads.get(1).split("\n").length);
        assertTrue(payloads.get(1).contains("\"score\":5") && payloads.get(1).contains("\"score\":6"));
    }

    @Test
    void consolidatedJsonUploadsTheWholeBatchOnce() throws Exception {
        service(false, true).addRecords(List.of(
                record(null, Map.of("id", 1), 0L), record(null, Map.of("id", 2, "score", 5), 1L)));

        String payload = new String(captureSingleUpload().payload, StandardCharsets.UTF_8);
        assertEquals(2, payload.split("\n").length);
    }

    @Test
    void explicitNullKeepsRecordInSameShapeAsPresentValue() throws Exception {
        // An explicit null is a present key (NULL is what the producer sent), unlike an absent key.
        Map<String, Object> withNull = new LinkedHashMap<>();
        withNull.put("id", 1);
        withNull.put("score", null);

        service(false).addRecords(List.of(record(null, withNull, 0L), record(null, Map.of("id", 2, "score", 3), 1L)));

        verify(statement, times(1)).execute(anyString(), anyMap());
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
        assertEquals("INSERT INTO \"t\" BY NAME SELECT *, 'my-batch' AS \"batch_id\" FROM read_json('upload://batch')", upload.sql);
    }

    @Test
    void skipsTombstonesAndUploadsNothingForEmptyBatch() throws Exception {
        service(false).addRecords(List.of(record(null, null, 0L)));
        verify(statement, never()).execute(anyString(), anyMap());
    }

    @Test
    void emptyJsonObjectIsShippedAsIs() throws Exception {
        // No connector-side field inspection: an empty object is uploaded like any other record and
        // Firebolt decides (defaults fill every column when other records in the batch carry fields).
        service(false).addRecords(List.of(record(null, Map.of(), 0L)));
        assertEquals("{}", new String(captureSingleUpload().payload, StandardCharsets.UTF_8).trim());
    }

    @Test
    void escapesLiteralColumnValuesAndNames() throws Exception {
        service(false).addRecords(List.of(record(null, Map.of("id", 1), 0L)), Map.of("we\"ird", "it's"));
        assertEquals("INSERT INTO \"t\" BY NAME SELECT *, 'it''s' AS \"we\"\"ird\" FROM read_json('upload://batch')",
                captureSingleUpload().sql);
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
