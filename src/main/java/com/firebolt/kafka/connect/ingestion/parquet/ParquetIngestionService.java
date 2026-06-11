package com.firebolt.kafka.connect.ingestion.parquet;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.IngestionService;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Passthrough ingestion: records are converted to Parquet exactly as Kafka typed them and
 * uploaded via {@code read_parquet('upload://...')}; all casting to the table's column
 * types happens server-side in the INSERT ... SELECT.
 *
 * <p>Records with a Connect schema (Avro, Protobuf, JSON-with-schema) are converted with
 * Confluent's {@link AvroData}, so the Parquet schema mirrors the Kafka schema including
 * logical types (decimal, date, timestamp). Schemaless records go through
 * {@link SchemalessAvroConverter}. A batch is grouped by value schema so a schema change
 * mid-batch produces one Parquet file per schema version.
 */
@Slf4j
public class ParquetIngestionService implements IngestionService {

    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO \"%s\" (%s) SELECT %s FROM read_parquet('upload://%s')";

    // there are no restrictions on the name of the multi part representing the parquet file. The only restriction is that it should match
    // this regex: [-A-Za-z0-9._~:\/?#\[\]@!$&'()*+,;=]+ and must be unique if multiple parts are send in the same request
    private static final String MULTIPART_FILENAME = "batch";

    /** group key for records without a value schema */
    private static final Object SCHEMALESS = new Object();

    private final Connection connection;
    private final TableSchema tableSchema;
    private final ErrorReporter errorReporter;
    private final boolean errorToleranceAll;
    private final AvroData avroData;
    private final SchemalessAvroConverter schemalessAvroConverter = new SchemalessAvroConverter();

    public ParquetIngestionService(Connection connection, ErrorReporter errorReporter, boolean errorToleranceAll, TableSchema tableSchema) {
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
            AvroBatch batch = group.getKey() == SCHEMALESS
                    ? schemalessAvroConverter.toAvro(group.getValue(), this::handleBadRecord)
                    : toAvro((Schema) group.getKey(), group.getValue());

            if (batch.getRecords().isEmpty()) {
                continue;
            }

            byte[] parquetBytes = writeParquet(batch);
            String sql = insertSql(batch, literalColumns);
            log.debug("Created the sql statement: {}. Parquet file has: {} bytes", sql, parquetBytes.length);
            execute(sql, parquetBytes);
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

    private AvroBatch toAvro(Schema connectSchema, List<SinkRecord> records) {
        org.apache.avro.Schema avroSchema = nonNullUnionBranch(avroData.fromConnectSchema(connectSchema));
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            records.forEach(record -> handleBadRecord(record, new RecordConversionException(
                    "Record value schema is not a struct: " + connectSchema.type())));
            return new AvroBatch(avroSchema, List.of(), Map.of());
        }

        List<GenericRecord> avroRecords = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            try {
                if (!(record.value() instanceof Struct)) {
                    throw new RecordConversionException(
                            "Record has a schema but its value is not a struct: " + record.value().getClass().getName());
                }
                avroRecords.add((GenericRecord) avroData.fromConnectData(connectSchema, record.value()));
            } catch (RuntimeException e) {
                handleBadRecord(record, e instanceof RecordConversionException ? e
                        : new RecordConversionException("Failed to convert record to Parquet representation", e));
            }
        }

        Map<String, String> fieldToSource = avroSchema.getFields().stream()
                .collect(Collectors.toMap(org.apache.avro.Schema.Field::name, org.apache.avro.Schema.Field::name,
                        (a, b) -> a, LinkedHashMap::new));
        return new AvroBatch(avroSchema, avroRecords, fieldToSource);
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

    private byte[] writeParquet(AvroBatch batch) throws SQLException {
        Configuration conf = new Configuration(false);

        // Required to support arrays with null elements
        // true → use the old, non-standard "two-level" list encoding
        // false → use the modern, spec-compliant "three-level" list encoding
        conf.setBoolean("parquet.avro.write-old-list-structure", false);

        InMemoryParquetFile file = new InMemoryParquetFile();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
                .withSchema(batch.getSchema())
                .withConf(conf)
                .build()) {
            for (GenericRecord record : batch.getRecords()) {
                writer.write(record);
            }
        } catch (Exception e) {
            throw new SQLException("Failed to write Parquet content in-memory", e);
        }
        return file.toByteArray();
    }

    private String insertSql(AvroBatch batch, Map<String, String> literalColumns) throws SQLException {
        Map<String, TableSchema.Column> columnsByLowerName = tableSchema.getColumns().stream()
                .collect(Collectors.toMap(column -> column.getName().toLowerCase(), Function.identity(), (a, b) -> a));

        List<String> insertColumns = new ArrayList<>();
        List<String> selectExpressions = new ArrayList<>();

        // only fields that match a table column (case-insensitively) are inserted; extras are dropped
        batch.getFieldToSourceName().forEach((avroField, sourceName) -> {
            TableSchema.Column column = columnsByLowerName.get(sourceName.toLowerCase());
            if (column != null) {
                insertColumns.add(quoteIdentifier(column.getName()));
                selectExpressions.add(quoteIdentifier(avroField));
            }
        });

        literalColumns.forEach((name, value) -> {
            TableSchema.Column column = columnsByLowerName.get(name.toLowerCase());
            if (column != null) {
                insertColumns.add(quoteIdentifier(column.getName()));
                selectExpressions.add("'" + value.replace("'", "''") + "'");
            }
        });

        if (insertColumns.isEmpty()) {
            throw new SQLException(String.format(
                    "No record fields match any column of table %s. Record fields: %s",
                    tableSchema.getTableName(), batch.getFieldToSourceName().values()));
        }

        return String.format(INSERT_SQL_TEMPLATE,
                tableSchema.getTableName(),
                String.join(", ", insertColumns),
                String.join(", ", selectExpressions),
                MULTIPART_FILENAME);
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private void execute(String sql, byte[] parquetBytes) throws SQLException {
        try {
            FireboltConnection fireboltConnection = connection.unwrap(FireboltConnection.class);
            try (FireboltParquetStatement statement = fireboltConnection.createParquetStatement()) {
                statement.execute(sql, Map.of(MULTIPART_FILENAME, parquetBytes));
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to upload parquet content", e);
        }
    }
}
