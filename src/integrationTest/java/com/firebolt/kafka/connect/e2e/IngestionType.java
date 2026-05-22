package com.firebolt.kafka.connect.e2e;

/** Ingestion mode for E2E tests. */
public enum IngestionType {
    SQL("sql"),
    BINARY("binary");

    private final String value;

    IngestionType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
