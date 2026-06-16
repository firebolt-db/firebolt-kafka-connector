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
        boolean manageTransaction = groups.size() > 1 && connection.getAutoCommit();
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

        List<GenericRecord> avroRecords = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            try {
                if (!(record.value() instanceof Struct)) {
                    throw new RecordConversionException("Record has a schema but its value is not a struct: " + record.value().getClass().getName());
                }
                avroRecords.add((GenericRecord) avroData.fromConnectData(connectSchema, record.value()));
            } catch (RuntimeException e) {
                handleBadRecord(record, e instanceof RecordConversionException ? e
                        : new RecordConversionException("Failed to convert record to Parquet representation", e));
            }
        }
        if (avroRecords.isEmpty()) {
            return;
        }

        List<String> fields = avroSchema.getFields().stream()
                .map(org.apache.avro.Schema.Field::name).collect(Collectors.toList());
        uploadAndInsert("read_parquet", writeParquet(avroSchema, avroRecords), fields, literalColumns);
    }

    /** Schemaless JSON records -> NDJSON -> read_json. */
    private void ingestJson(List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException {
        ByteArrayOutputStream ndjson = new ByteArrayOutputStream();
        Set<String> fields = new LinkedHashSet<>();
        boolean any = false;
        for (SinkRecord record : records) {
            if (!(record.value() instanceof Map)) {
                handleBadRecord(record, new RecordConversionException("Schemaless record value is not a JSON object: " + record.value().getClass().getName()));
                continue;
            }
            try {
                ndjson.write(objectMapper.writeValueAsBytes(record.value()));
                ndjson.write('\n');
            } catch (Exception e) {
                handleBadRecord(record, new RecordConversionException("Failed to serialize record to JSON", e));
                continue;
            }
            ((Map<?, ?>) record.value()).keySet().forEach(key -> fields.add(String.valueOf(key)));
            any = true;
        }
        if (!any) {
            return;
        }
        uploadAndInsert("read_json", ndjson.toByteArray(), new ArrayList<>(fields), literalColumns);
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
     * Builds {@code INSERT INTO t (<fields>) SELECT <fields> FROM <tvf>('upload://batch')} from the
     * record's own field names and runs it. Identifiers are quoted on both sides, so a field is
     * matched to the column whose name equals it exactly (case-sensitive) — the field name is the
     * column name. {@code literalColumns} (e.g. a batch id) are appended as constants.
     */
    private void uploadAndInsert(String tvf, byte[] payload, List<String> fields, Map<String, String> literalColumns) throws SQLException {
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
            // Every record in this group was an empty object (no fields) and no literal columns
            // apply — there is nothing to insert. Skip, but say so rather than silently dropping.
            log.warn("Skipping upload to {}: records have no fields to ingest", tableName);
            return;
        }

        String sql = String.format(INSERT_SQL_TEMPLATE, tableName,
                String.join(", ", insertColumns), String.join(", ", selectExpressions), tvf, MULTIPART_NAME);
        log.debug("Ingesting {} bytes via {}: {}", payload.length, tvf, sql);
        execute(sql, payload);
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
