package com.firebolt.kafka.connect.e2e;

import com.firebolt.kafka.connect.clients.FireboltClient;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import static org.awaitility.Awaitility.await;

/**
 * Orchestrates the full E2E test lifecycle:
 * setup → produce → wait → validate → cleanup.
 *
 * Uses the local Docker Compose infrastructure (Kafka, Schema Registry,
 * Kafka Connect, firebolt-core) from src/integrationTest/docker/.
 */
@Slf4j
public class E2ETestHarness {

    /** Default Docker Compose service endpoints (host-side mapped ports). */
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String DEFAULT_CONNECT_URL = "http://localhost:8083";

    /**
     * Schema Registry URL as seen from inside the Docker network.
     * Kafka Connect runs in Docker so it cannot reach Schema Registry via localhost;
     * it must use the Docker service hostname instead.
     */
    private static final String DOCKER_SCHEMA_REGISTRY_URL = "http://schema-registry:8081";

    private static final MediaType JSON_MEDIA = MediaType.parse("application/json");
    private static final OkHttpClient HTTP = new OkHttpClient.Builder()
            .connectTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(30))
            .build();

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String connectUrl;

    private E2ETestConfig config;
    private FireboltClient fireboltClient;
    private FireboltValidator validator;
    private MessageProducer producer;

    public E2ETestHarness() {
        this(DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_SCHEMA_REGISTRY_URL, DEFAULT_CONNECT_URL);
    }

    public E2ETestHarness(String bootstrapServers, String schemaRegistryUrl, String connectUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.connectUrl = connectUrl;
    }

    /**
     * Sets up the test: creates Kafka topic, deploys connector, creates Firebolt table.
     */
    public void setup(E2ETestConfig config) throws Exception {
        this.config = config;
        String topic = config.resolvedTopicName();
        String table = config.resolvedTableName();

        log.warn("[E2E] Setup: {} | topic={}", config.label(), topic);

        // 1. Create Kafka topic
        createKafkaTopic(topic);

        // 2. Connect to Firebolt and create target table
        this.fireboltClient = FireboltClient.createDefault();
        createFireboltTable(table);
        this.validator = new FireboltValidator(fireboltClient);

        // 3. Deploy Kafka Connect connector and wait until it is RUNNING
        deployConnector(config);
        awaitConnectorRunning(connectorName(config));

        // 4. Create message producer
        this.producer = MessageProducerFactory.create(
                config.getMessageType(), bootstrapServers, schemaRegistryUrl);

        log.warn("[E2E] Setup complete: topic={}, table={}, connector={}",
                topic, table, connectorName(config));
    }

    /** Max rows the producer can lead ingestion by before stalling. */
    private static final int MAX_PRODUCER_LEAD_ROWS = 10_000_000;
    /** Batch size for streaming record production. */
    private static final int PRODUCE_BATCH_SIZE = 1_000;

    /** Total records produced (set after production completes). */
    private volatile long totalProduced;

    public long getTotalProduced() {
        return totalProduced;
    }

    private volatile double produceDurationSeconds;
    private volatile double ingestDurationSeconds;

    /** Log production progress every N batches. */
    private static final int PROGRESS_LOG_INTERVAL_BATCHES = 10;

    /**
     * Produces records for the configured duration, then flushes.
     * Records are streamed in batches of {@code PRODUCE_BATCH_SIZE} with
     * backpressure: stalls when the producer leads ingestion by more than
     * {@code MAX_PRODUCER_LEAD_ROWS} rows.
     *
     * Logs throughput every {@code PROGRESS_LOG_INTERVAL_BATCHES} batches
     * so CI output shows live progress.
     */
    public void produceForDuration() {
        String topic = config.resolvedTopicName();
        String table = config.resolvedTableName();
        Duration targetDuration = config.getDuration();
        log.info("[PRODUCE] Starting: topic='{}', format={}, duration={}, batch={}, maxLead={}",
                topic, config.getMessageType(), targetDuration,
                PRODUCE_BATCH_SIZE, MAX_PRODUCER_LEAD_ROWS);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + targetDuration.toNanos();
        long produced = 0;
        int batchesSinceLog = 0;
        while (System.nanoTime() < deadlineNanos) {
            List<E2ETestRecord> batch = new ArrayList<>(PRODUCE_BATCH_SIZE);
            for (int i = 0; i < PRODUCE_BATCH_SIZE; i++) {
                batch.add(E2ETestRecord.forSequenceId(produced + i + 1, config.getRecordSizeBytes()));
            }
            producer.produce(topic, batch);
            produced += PRODUCE_BATCH_SIZE;
            batchesSinceLog++;

            if (batchesSinceLog >= PROGRESS_LOG_INTERVAL_BATCHES) {
                double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                double remainingSec = Math.max(0, (deadlineNanos - System.nanoTime()) / 1_000_000_000.0);
                log.info("[PRODUCE] {} records sent ({} rec/s) — {}s elapsed, {}s remaining",
                        produced, (int) (produced / elapsedSec),
                        String.format("%.1f", elapsedSec),
                        String.format("%.1f", remainingSec));
                batchesSinceLog = 0;
            }

            waitForBackpressure(table, produced);
        }
        producer.flush();
        long failed = producer.getFailedSendCount();
        totalProduced = produced - failed;
        if (failed > 0) {
            log.warn("[PRODUCE] {} sends failed; adjusting expected count to {}", failed, totalProduced);
        }

        produceDurationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        log.warn("[PRODUCE] Done: {} records to '{}' in {}s ({} rec/s)",
                produced, topic, String.format("%.1f", produceDurationSeconds),
                (int) (produced / produceDurationSeconds));
    }

    /**
     * Stalls if the producer has run too far ahead of Firebolt ingestion.
     * Skips the countRows query entirely when produced is safely below the limit.
     */
    private void waitForBackpressure(String table, long produced) {
        if (produced <= MAX_PRODUCER_LEAD_ROWS) return;
        try {
            long ingested = fireboltClient.countRows(table);
            long lead = produced - ingested;
            if (lead > MAX_PRODUCER_LEAD_ROWS) {
                log.info("Backpressure: produced={}, ingested={}, lead={} > {} — stalling",
                        produced, ingested, lead, MAX_PRODUCER_LEAD_ROWS);
                await()
                        .atMost(Duration.ofMinutes(5))
                        .pollInterval(Duration.ofSeconds(2))
                        .until(() -> {
                            long current = fireboltClient.countRows(table);
                            return (produced - current) <= MAX_PRODUCER_LEAD_ROWS / 2;
                        });
                log.info("Backpressure released, resuming production");
            }
        } catch (SQLException e) {
            log.debug("Backpressure check failed (non-fatal): {}", e.getMessage());
        }
    }

    /**
     * Polls Firebolt row count until all produced records have landed.
     * Must be called after {@link #produceForDuration()}.
     * Logs progress on every poll so CI shows live ingestion status.
     */
    public void waitForIngestion() {
        String table = config.resolvedTableName();
        long expected = totalProduced;
        long startNanos = System.nanoTime();
        log.warn("[INGEST] Waiting for {} rows in table '{}' (timeout={})...",
                expected, table, config.getIngestionTimeout());

        await()
                .atMost(config.getIngestionTimeout())
                .pollInterval(config.getPollInterval())
                .until(() -> {
                    try {
                        long count = fireboltClient.countRows(table);
                        double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                        int pct = expected > 0 ? (int) (100L * count / expected) : 100;
                        log.info("[INGEST] {}/{} rows ({}%) — {}s elapsed",
                                count, expected, pct,
                                String.format("%.1f", elapsedSec));
                        return count >= expected;
                    } catch (SQLException e) {
                        log.warn("[INGEST] Poll failed: {}", e.getMessage());
                        return false;
                    }
                });

        ingestDurationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        log.warn("[INGEST] Done: {} rows landed in {}s",
                expected, String.format("%.1f", ingestDurationSeconds));
        writeBenchmarkResult();
    }

    private void writeBenchmarkResult() {
        try {
            long produceRate = produceDurationSeconds > 0
                    ? (long) (totalProduced / produceDurationSeconds) : 0;
            double throughputMb = produceDurationSeconds > 0
                    ? (totalProduced * (double) config.getRecordSizeBytes()) / (1024.0 * 1024.0 * produceDurationSeconds)
                    : 0;
            long ingestRate = ingestDurationSeconds > 0
                    ? (long) (totalProduced / ingestDurationSeconds) : 0;

            BenchmarkResult result = BenchmarkResult.builder()
                    .commitSha(System.getenv().getOrDefault("GITHUB_SHA", "local"))
                    .timestamp(java.time.Instant.now().toString())
                    .durationSeconds(round1(produceDurationSeconds))
                    .totalRecordsProduced(totalProduced)
                    .produceRateRecordsPerSec(produceRate)
                    .produceThroughputMbPerSec(round1(throughputMb))
                    .ingestDurationSeconds(round1(ingestDurationSeconds))
                    .ingestRateRecordsPerSec(ingestRate)
                    .recordSizeBytes(config.getRecordSizeBytes())
                    .build();

            java.io.File outputDir = new java.io.File("build/reports/benchmark");
            outputDir.mkdirs();
            new com.fasterxml.jackson.databind.ObjectMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(new java.io.File(outputDir, "results.json"), result);

            log.warn("[BENCHMARK] produce={}rec/s ({}MB/s), ingest={}rec/s",
                    produceRate, String.format("%.1f", throughputMb), ingestRate);
        } catch (Exception e) {
            log.warn("[BENCHMARK] Failed to write results: {}", e.getMessage());
        }
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    /**
     * Validates that all produced records landed in Firebolt.
     * Enforces exact equality for exactly-once and "no data loss"
     * (count >= produced) for at-least-once.
     */
    public void validateRecordCount() throws SQLException {
        validator.validateRecordCount(
                config.resolvedTableName(), totalProduced, config.getDeliveryMode());
    }

    /**
     * Spot-checks data integrity in Firebolt.
     */
    public void validateDataIntegrity() throws SQLException {
        validator.validateDataIntegrity(config.resolvedTableName(), totalProduced);
    }

    /**
     * Tears down the test: removes connector, drops topic/table, closes clients.
     * Safe to call even when setup() was never invoked (e.g. skipped tests).
     */
    public void cleanup() {
        if (config == null) {
            return;
        }
        String connName = connectorName(config);
        String topic = config.resolvedTopicName();
        String table = config.resolvedTableName();

        log.warn("[E2E] Cleanup: connector={}, topic={}, table={}",
                connName, topic, table);

        // Close producer
        if (producer != null) {
            try {
                producer.close();
            } catch (IOException e) {
                log.warn("Failed to close producer: {}", e.getMessage());
            }
        }

        // Delete connector
        try {
            deleteConnector(connName);
        } catch (Exception e) {
            log.warn("Failed to delete connector '{}': {}", connName, e.getMessage());
        }

        // Delete Kafka topic
        try {
            deleteKafkaTopic(topic);
        } catch (Exception e) {
            log.warn("Failed to delete topic '{}': {}", topic, e.getMessage());
        }

        // Drop Firebolt table
        if (fireboltClient != null) {
            try {
                fireboltClient.dropTable(table);
            } catch (SQLException e) {
                log.warn("Failed to drop table '{}': {}", table, e.getMessage());
            }
            try {
                fireboltClient.close();
            } catch (SQLException e) {
                log.warn("Failed to close Firebolt client: {}", e.getMessage());
            }
        }

        log.warn("[E2E] Cleanup complete");
    }

    // --- Infrastructure helpers ---

    private void createKafkaTopic(String topicName) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            admin.createTopics(List.of(newTopic)).all().get();
            log.info("Created Kafka topic '{}'" , topicName);
        }
    }

    private void deleteKafkaTopic(String topicName) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            admin.deleteTopics(List.of(topicName)).all().get();
            log.info("Deleted Kafka topic '{}'", topicName);
        }
    }

    private void createFireboltTable(String tableName) throws SQLException {
        String ddl = String.format(
                "CREATE TABLE IF NOT EXISTS \"%s\" ("
                + "\"id\" BIGINT, "
                + "\"name\" TEXT, "
                + "\"value\" DOUBLE PRECISION, "
                + "\"timestamp\" TIMESTAMP)",
                tableName);
        fireboltClient.createTable(ddl);
        log.info("Created Firebolt table '{}'", tableName);
        awaitTableVisible(tableName);
    }

    /**
     * Polls until the freshly-created table is visible via SELECT.
     * Firebolt-core surfaces new tables to JDBC metadata asynchronously,
     * and the connector's table-existence validator races against that
     * propagation; this guard prevents flaky deploys.
     */
    private void awaitTableVisible(String tableName) {
        await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> {
                    try {
                        fireboltClient.countRows(tableName);
                        return true;
                    } catch (SQLException e) {
                        return false;
                    }
                });
    }

    private void deployConnector(E2ETestConfig config) throws IOException {
        String connName = connectorName(config);
        String topic = config.resolvedTopicName();
        String table = config.resolvedTableName();

        Map<String, String> connectorConfig = buildConnectorConfig(config, topic, table);

        // Build JSON body
        StringBuilder json = new StringBuilder();
        json.append("{\"name\":\"").append(connName).append("\",\"config\":{");
        boolean first = true;
        for (Map.Entry<String, String> entry : connectorConfig.entrySet()) {
            if (!first) json.append(",");
            json.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
            first = false;
        }
        json.append("}}");

        Request request = new Request.Builder()
                .url(connectUrl + "/connectors")
                .post(RequestBody.create(json.toString(), JSON_MEDIA))
                .build();

        try (Response response = HTTP.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to deploy connector '" + connName
                        + "': HTTP " + response.code() + " - " + body);
            }
            log.info("Deployed connector '{}'", connName);
        }
    }

    private void awaitConnectorRunning(String connectorName) throws IOException {
        log.info("[E2E] Waiting for connector '{}' to reach RUNNING state...", connectorName);
        await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> {
                    Request req = new Request.Builder()
                            .url(connectUrl + "/connectors/" + connectorName + "/status")
                            .get()
                            .build();
                    try (Response resp = HTTP.newCall(req).execute()) {
                        if (!resp.isSuccessful()) return false;
                        String body = resp.body().string();
                        return body.contains("\"RUNNING\"");
                    } catch (Exception e) {
                        return false;
                    }
                });
        log.warn("[E2E] Connector '{}' is RUNNING", connectorName);
    }

    private void deleteConnector(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(connectUrl + "/connectors/" + connectorName)
                .delete()
                .build();
        try (Response response = HTTP.newCall(request).execute()) {
            log.info("Deleted connector '{}': HTTP {}", connectorName, response.code());
        }
    }

    private Map<String, String> buildConnectorConfig(
            E2ETestConfig config, String topic, String table) {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");
        props.put("tasks.max", "1");
        props.put("topics", topic);
        props.put("topic.to.table.mapping", topic + ":" + table);
        props.put("jdbc.connection.url",
                "jdbc:firebolt:?url=http://firebolt-core:3473");
        props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");

        // Value converter based on message type
        configureValueConverter(props, config);

        // Delivery mode
        if (config.getDeliveryMode() == DeliveryMode.EXACTLY_ONCE) {
            props.put("exactlyOnce", "true");
        }

        // Ingestion type
        if (config.getIngestionType() == IngestionType.BINARY) {
            props.put("ingestion.type", "binary");
        }

        return props;
    }

    private void configureValueConverter(Map<String, String> props, E2ETestConfig config) {
        switch (config.getMessageType()) {
            case JSON:
                props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
                props.put("value.converter.schemas.enable", "false");
                break;
            case AVRO:
                props.put("value.converter", "io.confluent.connect.avro.AvroConverter");
                props.put("value.converter.schema.registry.url", DOCKER_SCHEMA_REGISTRY_URL);
                break;
            case PROTOBUF:
                props.put("value.converter",
                        "io.confluent.connect.protobuf.ProtobufConverter");
                props.put("value.converter.schema.registry.url", DOCKER_SCHEMA_REGISTRY_URL);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported message type: " + config.getMessageType());
        }
    }

    private static String connectorName(E2ETestConfig config) {
        return config.resolvedTopicName();
    }
}
