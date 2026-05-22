package com.firebolt.kafka.connect.e2e;

import java.time.Duration;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for a single E2E test scenario.
 * Defines the message format, delivery semantics, ingestion mode,
 * and timing parameters. All tests are duration-based: the producer
 * writes for a fixed window, then validation waits for all produced
 * data to land in Firebolt.
 */
@Getter
@Builder
public class E2ETestConfig {

    private final E2EMessageType messageType;
    private final DeliveryMode deliveryMode;
    private final IngestionType ingestionType;

    /**
     * How long the producer should write records.
     * Default is 3 seconds (~570K records at typical throughput).
     * Override with {@code -De2e.duration=PT30S} for longer soak runs.
     */
    @Builder.Default
    private final Duration duration = resolveDefaultDuration();

    private static Duration resolveDefaultDuration() {
        String override = System.getProperty("e2e.duration");
        return (override == null || override.isEmpty())
                ? Duration.ofSeconds(3)
                : Duration.parse(override);
    }

    /** Kafka topic name; auto-generated from config if not set. */
    private final String topicName;

    /** Firebolt target table name; auto-generated from config if not set. */
    private final String tableName;

    /** Average record size in bytes (pads the name field). */
    @Builder.Default
    private final int recordSizeBytes = 256;

    /**
     * Timeout for waiting for all produced records to land in Firebolt.
     * Override with {@code -De2e.ingestionTimeout=PT30S}.
     */
    @Builder.Default
    private final Duration ingestionTimeout = resolveDefaultIngestionTimeout();

    private static Duration resolveDefaultIngestionTimeout() {
        String override = System.getProperty("e2e.ingestionTimeout");
        return (override == null || override.isEmpty())
                ? Duration.ofMinutes(3)
                : Duration.parse(override);
    }

    /** Polling interval when checking Firebolt row count. */
    @Builder.Default
    private final Duration pollInterval = Duration.ofSeconds(5);

    /**
     * Random 8-char suffix shared by topic and table to ensure each
     * test run uses fresh, isolated names. Avoids cross-run contamination
     * if a previous run left state behind.
     */
    @Builder.Default
    private final String runId = UUID.randomUUID().toString().substring(0, 8);

    /**
     * Returns a resolved topic name: explicit value or deterministic default
     * with a run-unique suffix.
     */
    public String resolvedTopicName() {
        if (topicName != null && !topicName.isEmpty()) {
            return topicName;
        }
        return "e2e-" + messageType.getValue()
                + "-" + deliveryMode.getValue()
                + "-" + ingestionType.getValue()
                + "-" + runId;
    }

    /**
     * Returns a resolved table name: explicit value or deterministic default
     * with a run-unique suffix.
     */
    public String resolvedTableName() {
        if (tableName != null && !tableName.isEmpty()) {
            return tableName;
        }
        return "e2e_" + messageType.getValue()
                + "_" + deliveryMode.getValue()
                + "_" + ingestionType.getValue()
                + "_" + runId;
    }

    /**
     * Human-readable label for this config (used in test names and reports).
     */
    public String label() {
        return messageType.getValue() + " / " + deliveryMode.getValue() + " / " + ingestionType.getValue();
    }

    @Override
    public String toString() {
        return label();
    }
}
