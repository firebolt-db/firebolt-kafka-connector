package com.firebolt.kafka.connect.e2e;

/**
 * Ingestion mode for E2E tests. The connector has a single ingestion path (upload:// + read_avro/read_json;
 * {@code ingestion.type} is ignored), so there is one mode; the dimension is kept for the benchmark's cell labels.
 */
public enum IngestionType {
    SQL("sql");

    private final String value;

    IngestionType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
