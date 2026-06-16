package com.firebolt.kafka.connect.ingestion.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.IngestionService;
import com.firebolt.kafka.connect.TableSchema;
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
import java.util.function.Function;
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
 * Ships Kafka records to Firebolt as-is and lets the server parse them: records are
 * uploaded over the {@code upload://} HTTP primitive and ingested with an
 * {@code INSERT INTO <table> SELECT ... FROM read_xxx('upload://batch')}, so all type
 * casting happens server-side.
 *
 * <p>Two flows, by record shape:
 * <ul>
 *   <li><b>Schema-carrying records</b> (Avro, Protobuf, JSON-with-schema — delivered by the
 *   worker's converter as a Connect {@link Struct}) are written to Parquet with Confluent's
 *   {@link AvroData}, preserving logical types, and read back with {@code read_parquet}. A
 *   batch is grouped by value schema so a mid-batch schema change yields one file per schema.</li>
 *   <li><b>Schemaless records</b> (JSON with {@code schemas.enable=false}, delivered as a
 *   {@link Map}) are serialized straight back to NDJSON and read with {@code read_json},
 *   which infers types server-side — no type inference in the connector.</li>
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
    private final TableSchema tableSchema;
    private final ErrorReporter errorReporter;
    private final boolean errorToleranceAll;
    private final AvroData avroData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public UploadIngestionService(Connection connection, ErrorReporter errorReporter, boolean errorToleranceAll, TableSchema tableSchema) {
        this.connection = connection;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        this.tableSchema = tableSchema;
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

        for (Map.Entry<Object, List<SinkRecord>> group : groups.entrySet()) {
            if (group.getKey() == SCHEMALESS) {
                ingestJson(group.getValue(), literalColumns);
            } else {
                ingestParquet((Schema) group.getKey(), group.getValue(), literalColumns);
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
        }
        if (fields.isEmpty()) {
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
     * Builds {@code INSERT INTO t (cols) SELECT exprs FROM <tvf>('upload://batch')} and runs it.
     * Each upload field is matched case-insensitively to a table column; unmatched fields are
     * dropped and table columns absent from the upload default to NULL. {@code literalColumns}
     * (e.g. a batch id) are appended as constants when the table has the column.
     */
    private void uploadAndInsert(String tvf, byte[] payload, List<String> fields, Map<String, String> literalColumns) throws SQLException {
        Map<String, TableSchema.Column> columnsByLowerName = tableSchema.getColumns().stream()
                .collect(Collectors.toMap(column -> column.getName().toLowerCase(), Function.identity(), (a, b) -> a));

        List<String> insertColumns = new ArrayList<>();
        List<String> selectExpressions = new ArrayList<>();

        // Each projected value is CAST to the target column's exact type so the server does the
        // coercion (read_xxx-inferred types are not implicitly widened, e.g. numeric(10,2) -> numeric(38,2)).
        for (String field : fields) {
            TableSchema.Column column = columnsByLowerName.get(field.toLowerCase());
            if (column != null) {
                insertColumns.add(quoteIdentifier(column.getName()));
                selectExpressions.add(castExpression(quoteIdentifier(field), column));
            }
        }
        literalColumns.forEach((name, value) -> {
            TableSchema.Column column = columnsByLowerName.get(name.toLowerCase());
            if (column != null) {
                insertColumns.add(quoteIdentifier(column.getName()));
                selectExpressions.add(castExpression("'" + value.replace("'", "''") + "'", column));
            }
        });

        if (insertColumns.isEmpty()) {
            throw new SQLException(String.format(
                    "No record fields match any column of table %s. Record fields: %s", tableSchema.getTableName(), fields));
        }

        String sql = String.format(INSERT_SQL_TEMPLATE, tableSchema.getTableName(),
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

    private String castExpression(String valueExpression, TableSchema.Column column) {
        return "CAST(" + valueExpression + " AS " + column.getDataType() + ")";
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
