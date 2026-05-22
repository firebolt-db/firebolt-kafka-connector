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

    /** Default Docker Compose service endpoints. */
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String DEFAULT_CONNECT_URL = "http://localhost:8083";

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

        log.info("Setting up E2E test: {}", config.label());

        // 1. Create Kafka topic
        createKafkaTopic(topic);

        // 2. Connect to Firebolt and create target table
        this.fireboltClient = FireboltClient.createDefault();
        createFireboltTable(table);
        this.validator = new FireboltValidator(fireboltClient);

        // 3. Deploy Kafka Connect connector
        deployConnector(config);

        // 4. Create message producer
        this.producer = MessageProducerFactory.create(
                config.getMessageType(), bootstrapServers, schemaRegistryUrl);

        log.info("E2E setup complete: topic={}, table={}, connector={}",
                topic, table, connectorName(config));
    }

    /** Max rows the producer can lead ingestion by before stalling. */
    private static final int MAX_PRODUCER_LEAD_ROWS = 10_000_000;
    /** Batch size for streaming record production. */
    private static final int PRODUCE_BATCH_SIZE = 1_000;

    /** Total records produced (set after production completes). */
    private volatile int totalProduced;

    public int getTotalProduced() {
        return totalProduced;
    }

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
        int produced = 0;
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
        totalProduced = produced;

        double totalSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        log.info("[PRODUCE] Done: {} records to '{}' in {}s ({} rec/s)",
                produced, topic, String.format("%.1f", totalSec),
                (int) (produced / totalSec));
    }

    /**
     * Stalls if the producer has run too far ahead of Firebolt ingestion.
     */
    private void waitForBackpressure(String table, int produced) {
        try {
            int ingested = fireboltClient.countRows(table);
            int lead = produced - ingested;
            if (lead > MAX_PRODUCER_LEAD_ROWS) {
                log.info("Backpressure: produced={}, ingested={}, lead={} > {} — stalling",
                        produced, ingested, lead, MAX_PRODUCER_LEAD_ROWS);
                await()
                        .atMost(Duration.ofMinutes(5))
                        .pollInterval(Duration.ofSeconds(2))
                        .until(() -> {
                            int current = fireboltClient.countRows(table);
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
        int expected = totalProduced;
        long startNanos = System.nanoTime();
        log.info("[INGEST] Waiting for {} rows in table '{}' (timeout={})...",
                expected, table, config.getIngestionTimeout());

        await()
                .atMost(config.getIngestionTimeout())
                .pollInterval(config.getPollInterval())
                .until(() -> {
                    try {
                        int count = fireboltClient.countRows(table);
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

        double totalSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        log.info("[INGEST] Done: {} rows landed in {}s",
                expected, String.format("%.1f", totalSec));
    }

    /**
     * Validates that all produced records landed in Firebolt.
     */
    public void validateRecordCount() throws SQLException {
        validator.validateRecordCount(config.resolvedTableName(), totalProduced);
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

        log.info("Cleaning up E2E test: connector={}, topic={}, table={}",
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

        log.info("E2E cleanup complete");
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
                props.put("value.converter.schema.registry.url", schemaRegistryUrl);
                break;
            case PROTOBUF:
                props.put("value.converter",
                        "io.confluent.connect.protobuf.ProtobufConverter");
                props.put("value.converter.schema.registry.url", schemaRegistryUrl);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported message type: " + config.getMessageType());
        }
    }

    private static String connectorName(E2ETestConfig config) {
        return "e2e-" + config.getMessageType().getValue()
                + "-" + config.getDeliveryMode().getValue()
                + "-" + config.getIngestionType().getValue();
    }
}
