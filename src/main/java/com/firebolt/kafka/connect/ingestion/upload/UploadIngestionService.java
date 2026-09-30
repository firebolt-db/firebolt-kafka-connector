package com.firebolt.kafka.connect.ingestion.upload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.IngestionService;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Ships Kafka records to Firebolt as-is and lets the server parse them: records are uploaded
 * over the {@code upload://} HTTP primitive and ingested with
 * {@code INSERT INTO <table> BY NAME SELECT * FROM read_xxx('upload://batch')}.
 *
 * <p>The connector knows <b>no column names at all</b> — neither the table's nor the record's.
 * {@code BY NAME} makes Firebolt match each field the reader surfaces to the table column of the
 * same name (exact, case-sensitive) and apply its assignment casts. Consequences, by design:
 * <ul>
 *   <li>A record may carry a subset of the table's columns — absent columns take their default.
 *       (Firebolt-side schema evolution therefore needs no connector handling.)</li>
 *   <li>A field that is not a column of the table makes the batch fail — the table is the contract.</li>
 *   <li>Type coercion is exactly Firebolt's assignment-cast matrix; the connector adds none.</li>
 * </ul>
 *
 * <p>Two flows, by record shape:
 * <ul>
 *   <li><b>Schema-carrying records</b> (Avro, Protobuf, JSON-with-schema — delivered as a Connect
 *   {@link Struct}) are written to an Avro container file with Confluent's {@link AvroData} and read
 *   with {@code read_avro}. A batch is grouped by value schema so a mid-batch schema change yields
 *   one file per schema. {@code read_avro} honors Avro logical types, so Connect Timestamp/Date map
 *   to TIMESTAMP/DATE.</li>
 *   <li><b>Schemaless records</b> (JSON with {@code schemas.enable=false}, delivered as a
 *   {@link Map}) are serialized back to NDJSON and read with {@code read_json}, one upload per
 *   top-level key set so a key a record omits takes the column default rather than {@code NULL}.</li>
 * </ul>
 */
@Slf4j
public class UploadIngestionService implements IngestionService {

    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO \"%s\" BY NAME SELECT *%s FROM %s('upload://%s')";

    // The multipart part name referenced by upload://. Must match [_0-9a-zA-Z.-]+ and be unique per request.
    private static final String MULTIPART_NAME = "batch";

    // read_avro rejects decimals declaring precision > 38 (Firebolt's NUMERIC maximum), and AvroData
    // declares 64 for every Connect Decimal that carries no precision. The writer schema's precision is
    // capped at 38: it is metadata only (the unscaled bytes are unchanged), and a value that really has
    // more digits is still rejected by the engine when it reads the row.
    private static final int MAX_DECIMAL_PRECISION = 38;

    /** group key for schemaless records whose value is not a JSON object (reported as bad records) */
    private static final Object SCHEMALESS = new Object();

    /** group key for all schemaless JSON objects when uploads are consolidated */
    private static final Object JSON = new Object();

    private final Connection connection;
    private final String tableName;
    private final ErrorReporter errorReporter;
    private final boolean errorToleranceAll;
    private final boolean jsonConsolidateUploads;
    private final AvroData avroData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public UploadIngestionService(Connection connection, ErrorReporter errorReporter, boolean errorToleranceAll, String tableName,
                                  boolean jsonConsolidateUploads) {
        this.connection = connection;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        this.tableName = tableName;
        this.jsonConsolidateUploads = jsonConsolidateUploads;
        this.avroData = new AvroData(new AvroDataConfig(Map.of(
                AvroDataConfig.SCRUB_INVALID_NAMES_CONFIG, true,
                AvroDataConfig.CONNECT_META_DATA_CONFIG, false)));
    }

    @Override
    public void addRecords(List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        if (records == null || records.isEmpty()) {
            log.info("No records to ingest.");
            return;
        }

        Map<Object, List<SinkRecord>> groups = new LinkedHashMap<>();
        for (SinkRecord record : records) {
            if (record.value() == null) {
                log.debug("Skipping tombstone record: topic={}, partition={}, offset={}",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                continue;
            }
            groups.computeIfAbsent(groupKey(record), k -> new ArrayList<>()).add(record);
        }

        // A batch that mixes schemas (or schema'd + schemaless) becomes several INSERTs. Run them
        // in one transaction so a later failure can't leave earlier groups committed while Kafka
        // offsets are not advanced — which would duplicate those rows on retry. If a decorator
        // (post-processing) already owns the transaction (autoCommit already false), defer to it.
        // With error tolerance on, we instead let groups commit independently so split-and-retry can
        // land the good records and DLQ the bad ones (partial commit is the desired behavior there).
        boolean manageTransaction = groups.size() > 1 && connection.getAutoCommit() && !errorToleranceAll;
        if (manageTransaction) {
            connection.setAutoCommit(false);
        }
        try {
            for (Map.Entry<Object, List<SinkRecord>> group : groups.entrySet()) {
                if (group.getKey() instanceof Schema) {
                    ingestAvro((Schema) group.getKey(), group.getValue(), literalColumns);
                } else {
                    ingestJson(group.getValue(), literalColumns);
                }
            }
            if (manageTransaction) {
                connection.commit();
            }
        } catch (SQLException | RuntimeException e) {
            if (manageTransaction) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackError) {
                    log.error("Failed to roll back partial multi-group ingest", rollbackError);
                }
            }
            throw e;
        } finally {
            if (manageTransaction) {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException restoreError) {
                    log.error("Failed to restore auto-commit after multi-group ingest", restoreError);
                }
            }
        }
    }

    /**
     * Avro files are grouped by value schema. Schemaless JSON objects are grouped by their set of
     * top-level keys: {@code read_json} infers one schema per upload, so a key absent from one record but
     * present in another would surface as an explicit {@code NULL} for the former — overriding the
     * column's {@code DEFAULT}, or failing a {@code NOT NULL DEFAULT} column. Per key set, "absent" stays
     * absent and the default applies exactly as it would for that record on its own. This matters most
     * across a schema change, when one batch mixes pre- and post-change record shapes.
     * {@code json.consolidate.uploads=true} trades that correctness for one upload per batch.
     */
    private Object groupKey(SinkRecord record) {
        if (record.valueSchema() != null) {
            return record.valueSchema();
        }
        if (!(record.value() instanceof Map)) {
            return SCHEMALESS;
        }
        return jsonConsolidateUploads ? JSON : new HashSet<>(((Map<?, ?>) record.value()).keySet());
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Failed to gracefully close the ingestion service");
        }
    }

    /** Schema-carrying records -> Avro -> read_avro. */
    private void ingestAvro(Schema connectSchema, List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        org.apache.avro.Schema avroSchema = capDecimalPrecision(nonNullUnionBranch(avroData.fromConnectSchema(connectSchema)));
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            for (SinkRecord record : records) {
                handleBadRecord(record, new RecordConversionException("Record value schema is not a struct: " + connectSchema.type()));
            }
            return;
        }

        List<SinkRecord> convertible = new ArrayList<>(records.size());
        List<GenericRecord> avroRecords = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            try {
                if (!(record.value() instanceof Struct)) {
                    throw new RecordConversionException("Record has a schema but its value is not a struct: " + record.value().getClass().getName());
                }
                avroRecords.add((GenericRecord) avroData.fromConnectData(connectSchema, record.value()));
                convertible.add(record);
            } catch (RuntimeException e) {
                handleBadRecord(record, e instanceof RecordConversionException ? e
                        : new RecordConversionException("Failed to convert record to Avro representation", e));
            }
        }
        if (convertible.isEmpty()) {
            return;
        }

        uploadWithIsolation("read_avro", convertible, literalColumns,
                (from, to) -> writeAvro(avroSchema, avroRecords.subList(from, to)), 0, convertible.size());
    }

    /** Schemaless JSON records -> NDJSON -> read_json. */
    private void ingestJson(List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        List<SinkRecord> convertible = new ArrayList<>(records.size());
        List<byte[]> lines = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            if (!(record.value() instanceof Map)) {
                handleBadRecord(record, new RecordConversionException("Schemaless record value is not a JSON object: " + record.value().getClass().getName()));
                continue;
            }
            try {
                lines.add(objectMapper.writeValueAsBytes(record.value()));
            } catch (Exception e) {
                handleBadRecord(record, new RecordConversionException("Failed to serialize record to JSON", e));
                continue;
            }
            convertible.add(record);
        }
        if (convertible.isEmpty()) {
            return;
        }
        uploadWithIsolation("read_json", convertible, literalColumns, (from, to) -> {
            ByteArrayOutputStream ndjson = new ByteArrayOutputStream();
            try {
                for (int i = from; i < to; i++) {
                    ndjson.write(lines.get(i));
                    ndjson.write('\n');
                }
            } catch (IOException e) {
                throw new SQLException("Failed to assemble NDJSON batch", e);
            }
            return ndjson.toByteArray();
        }, 0, convertible.size());
    }

    /** AvroData maps an optional struct schema to a [null, record] union; the writer needs the record branch. */
    private org.apache.avro.Schema nonNullUnionBranch(org.apache.avro.Schema schema) {
        if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(branch -> branch.getType() != org.apache.avro.Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
    }

    /** Caps every decimal's declared precision at {@value #MAX_DECIMAL_PRECISION} (see the constant). */
    private org.apache.avro.Schema capDecimalPrecision(org.apache.avro.Schema schema) {
        String json = schema.toString();
        if (!json.contains("\"decimal\"")) {
            return schema;
        }
        try {
            JsonNode tree = objectMapper.readTree(json);
            capDecimalPrecision(tree);
            return new org.apache.avro.Schema.Parser().parse(tree.toString());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to rewrite Avro schema " + json, e);
        }
    }

    private static void capDecimalPrecision(JsonNode node) {
        if ("decimal".equals(node.path("logicalType").asText()) && node.path("precision").asInt() > MAX_DECIMAL_PRECISION) {
            ((ObjectNode) node).put("precision", MAX_DECIMAL_PRECISION);
        }
        node.forEach(UploadIngestionService::capDecimalPrecision);
    }

    private byte[] writeAvro(org.apache.avro.Schema avroSchema, List<GenericRecord> records) throws SQLException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
            // Snappy: cheap CPU on the (hot-path) worker, good ratio. See specs/format-benchmark-results.md.
            writer.setCodec(CodecFactory.snappyCodec());
            writer.create(avroSchema, buffer);
            for (GenericRecord record : records) {
                writer.append(record);
            }
        } catch (IOException e) {
            throw new SQLException("Failed to write Avro content in-memory", e);
        }
        return buffer.toByteArray();
    }

    /**
     * Executes the INSERT for records[from, to). On failure, when error tolerance is enabled, splits
     * the range and retries each half, isolating an offending record to the DLQ at size 1. This keeps
     * a single bad record (or an oversized upload) from failing the whole batch; without tolerance the
     * failure propagates and the task fails.
     */
    private void uploadWithIsolation(String tvf, List<SinkRecord> records, Map<String, String> literalColumns,
                                     RangeAssembler assembler, int from, int to) throws SQLException {
        byte[] payload = assembler.assemble(from, to);
        try {
            log.debug("Ingesting {} record(s), {} bytes via {}", to - from, payload.length, tvf);
            execute(buildInsertSql(tvf, literalColumns), payload);
        } catch (SQLException e) {
            if (!errorToleranceAll) {
                throw e;
            }
            if (to - from == 1) {
                log.warn("Record at partition {} offset {} rejected by Firebolt; sending to the dead letter queue",
                        records.get(from).kafkaPartition(), records.get(from).kafkaOffset(), e);
                errorReporter.report(records.get(from), e);
                return;
            }
            // Split and retry to isolate the offending record(s).
            int mid = (from + to) >>> 1;
            uploadWithIsolation(tvf, records, literalColumns, assembler, from, mid);
            uploadWithIsolation(tvf, records, literalColumns, assembler, mid, to);
        }
    }

    /**
     * Builds {@code INSERT INTO t BY NAME SELECT * FROM <tvf>('upload://batch')}. Firebolt matches the
     * reader's columns to the table's by name, so the connector never lists a column. {@code literalColumns}
     * (e.g. a batch id) are appended as named constants; a record that carries a field of the same name
     * is rejected by Firebolt ("matches the target column more than once") rather than silently letting
     * one value win.
     */
    private String buildInsertSql(String tvf, Map<String, String> literalColumns) {
        StringBuilder literals = new StringBuilder();
        literalColumns.forEach((name, value) -> literals.append(", '").append(value.replace("'", "''"))
                .append("' AS ").append(quoteIdentifier(name)));
        return String.format(INSERT_SQL_TEMPLATE, tableName, literals, tvf, MULTIPART_NAME);
    }

    /** Assembles the upload payload for a sub-range of the batch (used by split-and-retry). */
    @FunctionalInterface
    private interface RangeAssembler {
        byte[] assemble(int from, int to) throws SQLException;
    }

    private void handleBadRecord(SinkRecord record, RuntimeException cause) {
        log.error("Error converting record: topic={}, partition={}, offset={}",
                record.topic(), record.kafkaPartition(), record.kafkaOffset(), cause);
        if (!errorToleranceAll) {
            throw cause;
        }
        errorReporter.report(record, cause);
        log.warn("Record from partition {} at offset {} will be submitted to the dead letter queue",
                record.kafkaPartition(), record.kafkaOffset());
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private void execute(String sql, byte[] payload) throws SQLException {
        try {
            FireboltConnection fireboltConnection = connection.unwrap(FireboltConnection.class);
            try (FireboltParquetStatement statement = fireboltConnection.createParquetStatement()) {
                statement.execute(sql, Map.of(MULTIPART_NAME, payload));
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to upload content to Firebolt", e);
        }
    }
}
