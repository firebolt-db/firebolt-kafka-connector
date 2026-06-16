package com.firebolt.kafka.connect.ingestion.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.IngestionService;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * Ships Kafka records to Firebolt as-is and lets the server parse them: records are uploaded
 * over the {@code upload://} HTTP primitive and ingested with an
 * {@code INSERT INTO <table> (<record fields>) SELECT <record fields> FROM read_xxx('upload://batch')}.
 *
 * <p>The connector holds <b>no table schema</b>: the column list is built from each record's own
 * field names, and Firebolt applies its assignment casts and resolves the columns. Consequences,
 * by design:
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
 *   {@link Struct}) are written to Parquet with Confluent's {@link AvroData} and read with
 *   {@code read_parquet}. A batch is grouped by value schema so a mid-batch schema change yields
 *   one file per schema.</li>
 *   <li><b>Schemaless records</b> (JSON with {@code schemas.enable=false}, delivered as a
 *   {@link Map}) are serialized back to NDJSON and read with {@code read_json}.</li>
 * </ul>
 */
@Slf4j
public class UploadIngestionService implements IngestionService {

    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO \"%s\" (%s) SELECT %s FROM %s('upload://%s')";

    // The multipart part name referenced by upload://. Must match [_0-9a-zA-Z.-]+ and be unique per request.
    private static final String MULTIPART_NAME = "batch";

    /** group key for records without a value schema */
    private static final Object SCHEMALESS = new Object();

    private final Connection connection;
    private final String tableName;
    private final ErrorReporter errorReporter;
    private final boolean errorToleranceAll;
    private final AvroData avroData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public UploadIngestionService(Connection connection, ErrorReporter errorReporter, boolean errorToleranceAll, String tableName) {
        this.connection = connection;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        this.tableName = tableName;
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
            groups.computeIfAbsent(record.valueSchema() == null ? SCHEMALESS : record.valueSchema(), k -> new ArrayList<>())
                    .add(record);
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
                if (group.getKey() == SCHEMALESS) {
                    ingestJson(group.getValue(), literalColumns);
                } else {
                    ingestParquet((Schema) group.getKey(), group.getValue(), literalColumns);
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

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Failed to gracefully close the ingestion service");
        }
    }

    /** Schema-carrying records -> Parquet -> read_parquet. */
    private void ingestParquet(Schema connectSchema, List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        org.apache.avro.Schema avroSchema = nonNullUnionBranch(avroData.fromConnectSchema(connectSchema));
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
                        : new RecordConversionException("Failed to convert record to Parquet representation", e));
            }
        }
        if (convertible.isEmpty()) {
            return;
        }

        List<String> fields = avroSchema.getFields().stream()
                .map(org.apache.avro.Schema.Field::name).collect(Collectors.toList());
        uploadWithIsolation("read_parquet", convertible, literalColumns,
                (from, to) -> new Payload(writeParquet(avroSchema, avroRecords.subList(from, to)), fields),
                0, convertible.size());
    }

    /** Schemaless JSON records -> NDJSON -> read_json. */
    private void ingestJson(List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        List<SinkRecord> convertible = new ArrayList<>(records.size());
        List<byte[]> lines = new ArrayList<>(records.size());
        List<Set<String>> keysPerRecord = new ArrayList<>(records.size());
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
            Set<String> keys = new LinkedHashSet<>();
            ((Map<?, ?>) record.value()).keySet().forEach(key -> keys.add(String.valueOf(key)));
            keysPerRecord.add(keys);
            convertible.add(record);
        }
        if (convertible.isEmpty()) {
            return;
        }
        uploadWithIsolation("read_json", convertible, literalColumns, (from, to) -> {
            ByteArrayOutputStream ndjson = new ByteArrayOutputStream();
            Set<String> fields = new LinkedHashSet<>();
            try {
                for (int i = from; i < to; i++) {
                    ndjson.write(lines.get(i));
                    ndjson.write('\n');
                    fields.addAll(keysPerRecord.get(i));
                }
            } catch (java.io.IOException e) {
                throw new SQLException("Failed to assemble NDJSON batch", e);
            }
            return new Payload(ndjson.toByteArray(), new ArrayList<>(fields));
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

    private byte[] writeParquet(org.apache.avro.Schema avroSchema, List<GenericRecord> records) throws SQLException {
        Configuration conf = new Configuration(false);
        // Use the spec-compliant three-level list encoding so arrays with null elements round-trip.
        conf.setBoolean("parquet.avro.write-old-list-structure", false);

        InMemoryParquetFile file = new InMemoryParquetFile();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
                .withSchema(avroSchema)
                .withConf(conf)
                .build()) {
            for (GenericRecord record : records) {
                writer.write(record);
            }
        } catch (Exception e) {
            throw new SQLException("Failed to write Parquet content in-memory", e);
        }
        return file.toByteArray();
    }

    /**
     * Executes the INSERT for records[from, to). On failure, when error tolerance is enabled, splits
     * the range and retries each half, isolating an offending record to the DLQ at size 1. This keeps
     * a single bad record (or an oversized upload) from failing the whole batch; without tolerance the
     * failure propagates and the task fails.
     */
    private void uploadWithIsolation(String tvf, List<SinkRecord> records, Map<String, String> literalColumns,
                                     RangeAssembler assembler, int from, int to) throws SQLException {
        Payload payload = assembler.assemble(from, to);
        String sql = buildInsertSql(tvf, payload.fields, literalColumns);
        if (sql == null) {
            return;
        }
        try {
            log.debug("Ingesting {} record(s), {} bytes via {}", to - from, payload.bytes.length, tvf);
            execute(sql, payload.bytes);
        } catch (SQLException e) {
            if (!errorToleranceAll || to - from <= 1) {
                if (errorToleranceAll && to - from == 1) {
                    log.warn("Record at partition {} offset {} rejected by Firebolt; sending to the dead letter queue",
                            records.get(from).kafkaPartition(), records.get(from).kafkaOffset(), e);
                    errorReporter.report(records.get(from), e);
                    return;
                }
                throw e;
            }
            // Split and retry to isolate the offending record(s).
            int mid = (from + to) >>> 1;
            uploadWithIsolation(tvf, records, literalColumns, assembler, from, mid);
            uploadWithIsolation(tvf, records, literalColumns, assembler, mid, to);
        }
    }

    /**
     * Builds {@code INSERT INTO t (<fields>) SELECT <fields> FROM <tvf>('upload://batch')} from the
     * record's own field names. Identifiers are quoted on both sides, so a field is matched to the
     * column whose name equals it exactly (case-sensitive) — the field name is the column name.
     * {@code literalColumns} (e.g. a batch id) are appended as constants. Returns null when there is
     * nothing to insert (e.g. all records in range were empty objects).
     */
    private String buildInsertSql(String tvf, List<String> fields, Map<String, String> literalColumns) {
        List<String> insertColumns = new ArrayList<>();
        List<String> selectExpressions = new ArrayList<>();

        for (String field : fields) {
            insertColumns.add(quoteIdentifier(field));
            selectExpressions.add(quoteIdentifier(field));
        }
        literalColumns.forEach((name, value) -> {
            // Don't emit a column twice if a record field collides with a literal (e.g. a record
            // that already carries a "batch_id"); the record's own value wins.
            if (fields.contains(name)) {
                return;
            }
            insertColumns.add(quoteIdentifier(name));
            selectExpressions.add("'" + value.replace("'", "''") + "'");
        });

        if (insertColumns.isEmpty()) {
            log.warn("Skipping upload to {}: records have no fields to ingest", tableName);
            return null;
        }

        return String.format(INSERT_SQL_TEMPLATE, tableName,
                String.join(", ", insertColumns), String.join(", ", selectExpressions), tvf, MULTIPART_NAME);
    }

    /** Assembles the upload payload + field list for a sub-range of the batch (used by split-and-retry). */
    @FunctionalInterface
    private interface RangeAssembler {
        Payload assemble(int from, int to) throws SQLException;
    }

    private static final class Payload {
        final byte[] bytes;
        final List<String> fields;

        Payload(byte[] bytes, List<String> fields) {
            this.bytes = bytes;
            this.fields = fields;
        }
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
