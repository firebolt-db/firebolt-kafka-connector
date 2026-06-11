package com.firebolt.kafka.connect.ingestion.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Date;
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

class ParquetIngestionServiceTest {

    private static final String TOPIC = "events";

    private Connection connection;
    private FireboltParquetStatement parquetStatement;
    private ErrorReporter errorReporter;

    @BeforeEach
    void setUp() throws Exception {
        connection = mock(Connection.class);
        FireboltConnection fireboltConnection = mock(FireboltConnection.class);
        parquetStatement = mock(FireboltParquetStatement.class);
        when(connection.unwrap(FireboltConnection.class)).thenReturn(fireboltConnection);
        when(fireboltConnection.createParquetStatement()).thenReturn(parquetStatement);
    }

    private ParquetIngestionService service(TableSchema tableSchema, boolean errorToleranceAll) {
        errorReporter = mock(ErrorReporter.class);
        return new ParquetIngestionService(connection, errorReporter, errorToleranceAll, tableSchema);
    }

    private SinkRecord record(Schema valueSchema, Object value, long offset) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, offset);
    }

    // --- schema'd records ---

    @Test
    void shouldRoundTripLogicalAndNestedTypesThroughParquet() throws Exception {
        Schema addressSchema = SchemaBuilder.struct().name("Address")
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct().name("Event")
                .field("id", Schema.INT64_SCHEMA)
                .field("amount", Decimal.schema(2))
                .field("created_at", Timestamp.SCHEMA)
                .field("birth_date", Date.SCHEMA)
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("address", addressSchema)
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        java.util.Date createdAt = new java.util.Date(1718000000000L);
        java.util.Date birthDate = Date.toLogical(Date.SCHEMA, 19000);
        Struct value = new Struct(valueSchema)
                .put("id", 42L)
                .put("amount", new BigDecimal("1234.56"))
                .put("created_at", createdAt)
                .put("birth_date", birthDate)
                .put("tags", List.of("a", "b"))
                .put("address", new Struct(addressSchema).put("city", "tlv").put("zip", 12345))
                .put("note", null);

        TableSchema tableSchema = new TableSchema("events_table");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        tableSchema.addColumn("amount", "numeric(38,2)", Types.NUMERIC, true);
        tableSchema.addColumn("created_at", "timestamp", Types.TIMESTAMP, true);
        tableSchema.addColumn("birth_date", "date", Types.DATE, true);
        tableSchema.addColumn("tags", "array(text)", Types.ARRAY, true);
        tableSchema.addColumn("address", "text", Types.VARCHAR, true);
        tableSchema.addColumn("note", "text", Types.VARCHAR, true);

        service(tableSchema, false).addRecords(List.of(record(valueSchema, value, 7L)));

        UploadedBatch batch = captureSingleUpload();
        assertEquals("INSERT INTO \"events_table\" (\"id\", \"amount\", \"created_at\", \"birth_date\", \"tags\", \"address\", \"note\") "
                + "SELECT \"id\", \"amount\", \"created_at\", \"birth_date\", \"tags\", \"address\", \"note\" "
                + "FROM read_parquet('upload://batch')", batch.sql);

        List<GenericRecord> rows = readParquet(batch.parquetBytes);
        assertEquals(1, rows.size());
        GenericRecord row = rows.get(0);

        assertEquals(42L, row.get("id"));
        // the reader applies Avro logical-type conversions, proving the parquet file carries them
        assertEquals(java.time.Instant.ofEpochMilli(1718000000000L), row.get("created_at"));
        assertEquals("timestamp-millis", logicalTypeName(row, "created_at"));
        assertEquals(java.time.LocalDate.ofEpochDay(19000), row.get("birth_date"));
        assertEquals("date", logicalTypeName(row, "birth_date"));
        assertNull(row.get("note"));

        org.apache.avro.Schema amountSchema = nonNull(row.getSchema().getField("amount").schema());
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) amountSchema.getLogicalType();
        assertEquals(2, decimalType.getScale());
        Object rawAmount = row.get("amount");
        BigDecimal amount = rawAmount instanceof ByteBuffer
                ? new Conversions.DecimalConversion().fromBytes((ByteBuffer) rawAmount, amountSchema, decimalType)
                : (BigDecimal) rawAmount;
        assertEquals(new BigDecimal("1234.56"), amount);

        assertEquals(List.of("a", "b"), ((List<?>) row.get("tags")).stream().map(Object::toString).collect(java.util.stream.Collectors.toList()));
        GenericRecord address = (GenericRecord) row.get("address");
        assertEquals("tlv", address.get("city").toString());
        assertEquals(12345, address.get("zip"));
    }

    @Test
    void shouldMatchColumnsCaseInsensitivelyAndDropUnknownFields() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event")
                .field("UserId", Schema.INT64_SCHEMA)
                .field("not_in_table", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("UserId", 1L).put("not_in_table", "x");

        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("userid", "bigint", Types.BIGINT, false);

        service(tableSchema, false).addRecords(List.of(record(valueSchema, value, 0L)));

        UploadedBatch batch = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"userid\") SELECT \"UserId\" FROM read_parquet('upload://batch')", batch.sql);
    }

    @Test
    void shouldSplitBatchOnSchemaChange() throws Exception {
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

        verify(parquetStatement, times(2)).execute(anyString(), anyMap());
    }

    @Test
    void shouldAddLiteralColumnsOnlyWhenTableHasThem() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();

        TableSchema withBatchId = new TableSchema("t");
        withBatchId.addColumn("a", "bigint", Types.BIGINT, false);
        withBatchId.addColumn("batch_id", "text", Types.VARCHAR, true);

        service(withBatchId, false).addRecords(
                List.of(record(valueSchema, new Struct(valueSchema).put("a", 1L), 0L)),
                Map.of("batch_id", "my-batch", "not_a_column", "ignored"));

        UploadedBatch batch = captureSingleUpload();
        assertEquals("INSERT INTO \"t\" (\"a\", \"batch_id\") SELECT \"a\", 'my-batch' FROM read_parquet('upload://batch')", batch.sql);
    }

    @Test
    void shouldSkipTombstonesAndUploadNothingForEmptyBatch() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("a", "bigint", Types.BIGINT, false);

        service(tableSchema, false).addRecords(List.of(record(valueSchema, null, 0L)));

        verify(parquetStatement, never()).execute(anyString(), anyMap());
    }

    @Test
    void shouldFailWhenNoFieldsMatchAnyTableColumn() {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("completely_different", "bigint", Types.BIGINT, false);

        assertThrows(SQLException.class, () -> service(tableSchema, false)
                .addRecords(List.of(record(valueSchema, new Struct(valueSchema).put("a", 1L), 0L))));
    }

    @Test
    void shouldReportBadRecordToDlqWhenErrorToleranceAll() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("a", "bigint", Types.BIGINT, false);

        SinkRecord good = record(valueSchema, new Struct(valueSchema).put("a", 1L), 0L);
        SinkRecord bad = record(valueSchema, "not a struct", 1L);

        service(tableSchema, true).addRecords(List.of(good, bad));

        verify(errorReporter).report(any(SinkRecord.class), any(Exception.class));
        UploadedBatch batch = captureSingleUpload();
        assertEquals(1, readParquet(batch.parquetBytes).size());
    }

    @Test
    void shouldThrowOnBadRecordWhenErrorToleranceNone() {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("a", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("a", "bigint", Types.BIGINT, false);

        SinkRecord bad = record(valueSchema, "not a struct", 0L);

        assertThrows(RecordConversionException.class,
                () -> service(tableSchema, false).addRecords(List.of(bad)));
        verify(errorReporter, never()).report(any(SinkRecord.class), any(Exception.class));
    }

    // --- schemaless records ---

    @Test
    void shouldIngestSchemalessMapsWithMechanicalTypeMapping() throws Exception {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("id", 1);
        first.put("score", 10L);
        first.put("active", true);
        first.put("tags", List.of("x", "y"));
        first.put("nested", Map.of("k", "v"));
        first.put("missing_in_second", "present");

        Map<String, Object> second = new LinkedHashMap<>();
        second.put("id", 2);
        second.put("score", 1.5); // promotes score long -> double
        second.put("active", false);
        second.put("tags", List.of("z"));
        second.put("nested", Map.of("k2", 3));

        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);
        tableSchema.addColumn("score", "double", Types.DOUBLE, true);
        tableSchema.addColumn("active", "boolean", Types.BOOLEAN, true);
        tableSchema.addColumn("tags", "array(text)", Types.ARRAY, true);
        tableSchema.addColumn("nested", "text", Types.VARCHAR, true);
        tableSchema.addColumn("missing_in_second", "text", Types.VARCHAR, true);

        service(tableSchema, false).addRecords(List.of(record(null, first, 0L), record(null, second, 1L)));

        UploadedBatch batch = captureSingleUpload();
        List<GenericRecord> rows = readParquet(batch.parquetBytes);
        assertEquals(2, rows.size());

        assertEquals(1L, rows.get(0).get("id"));
        assertEquals(10.0, rows.get(0).get("score"));
        assertEquals(true, rows.get(0).get("active"));
        assertEquals("{\"k\":\"v\"}", rows.get(0).get("nested").toString());
        assertEquals("present", rows.get(0).get("missing_in_second").toString());

        assertEquals(2L, rows.get(1).get("id"));
        assertEquals(1.5, rows.get(1).get("score"));
        assertNull(rows.get(1).get("missing_in_second"));
    }

    @Test
    void shouldReportSchemalessNonMapRecordToDlq() throws Exception {
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);

        SinkRecord good = record(null, Map.of("id", 1), 0L);
        SinkRecord bad = record(null, "just a string", 1L);

        service(tableSchema, true).addRecords(List.of(good, bad));

        verify(errorReporter).report(any(SinkRecord.class), any(Exception.class));
        assertEquals(1, readParquet(captureSingleUpload().parquetBytes).size());
    }

    @Test
    void shouldIngestMixedSchemaAndSchemalessBatchSeparately() throws Exception {
        Schema valueSchema = SchemaBuilder.struct().name("Event").field("id", Schema.INT64_SCHEMA).build();
        TableSchema tableSchema = new TableSchema("t");
        tableSchema.addColumn("id", "bigint", Types.BIGINT, false);

        service(tableSchema, false).addRecords(List.of(
                record(valueSchema, new Struct(valueSchema).put("id", 1L), 0L),
                record(null, Map.of("id", 2), 1L)));

        verify(parquetStatement, times(2)).execute(anyString(), anyMap());
    }

    // --- helpers ---

    private static final class UploadedBatch {
        final String sql;
        final byte[] parquetBytes;

        UploadedBatch(String sql, byte[] parquetBytes) {
            this.sql = sql;
            this.parquetBytes = parquetBytes;
        }
    }

    private UploadedBatch captureSingleUpload() throws SQLException {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, byte[]>> filesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(parquetStatement).execute(sqlCaptor.capture(), filesCaptor.capture());
        return new UploadedBatch(sqlCaptor.getValue(), filesCaptor.getValue().get("batch"));
    }

    private String logicalTypeName(GenericRecord row, String field) {
        org.apache.avro.LogicalType logicalType = nonNull(row.getSchema().getField(field).schema()).getLogicalType();
        return logicalType == null ? null : logicalType.getName();
    }

    private org.apache.avro.Schema nonNull(org.apache.avro.Schema schema) {
        if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                .findFirst().orElseThrow();
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
                    // position derived from available() so it stays correct for every read path
                    return bytes.length - stream.available();
                }

                @Override
                public void seek(long newPos) throws IOException {
                    stream.reset();
                    long skipped = stream.skip(newPos);
                    if (skipped != newPos) {
                        throw new IOException("Could not seek to position " + newPos);
                    }
                }
            };
        }
    }
}
