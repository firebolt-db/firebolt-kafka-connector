package com.firebolt.kafka.connect.e2e;

import java.time.Duration;
import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for a single E2E test scenario.
 * Defines the message format, delivery semantics, ingestion mode,
 * and test parameters for one cell in the config matrix.
 */
@Getter
@Builder
public class E2ETestConfig {

    private final E2EMessageType messageType;
    private final DeliveryMode deliveryMode;
    private final IngestionType ingestionType;

    /** Number of records to produce for correctness tests. */
    @Builder.Default
    private final int recordCount = 10_000;

    /** Duration for throughput benchmarks; null for correctness tests. */
    private final Duration duration;

    /** Kafka topic name; auto-generated from config if not set. */
    private final String topicName;

    /** Firebolt target table name; auto-generated from config if not set. */
    private final String tableName;

    /** Average record size in bytes for throughput tests (pads the name field). */
    @Builder.Default
    private final int recordSizeBytes = 256;

    /** Timeout for waiting for all records to land in Firebolt. */
    @Builder.Default
    private final Duration ingestionTimeout = Duration.ofMinutes(5);

    /** Polling interval when checking Firebolt row count. */
    @Builder.Default
    private final Duration pollInterval = Duration.ofSeconds(5);

    /**
     * Returns a resolved topic name: explicit value or deterministic default.
     */
    public String resolvedTopicName() {
        if (topicName != null && !topicName.isEmpty()) {
            return topicName;
        }
        return "e2e-" + messageType.getValue()
                + "-" + deliveryMode.getValue()
                + "-" + ingestionType.getValue();
    }

    /**
     * Returns a resolved table name: explicit value or deterministic default.
     */
    public String resolvedTableName() {
        if (tableName != null && !tableName.isEmpty()) {
            return tableName;
        }
        // Firebolt tables use underscores, not hyphens
        return "e2e_" + messageType.getValue()
                + "_" + deliveryMode.getValue()
                + "_" + ingestionType.getValue();
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
