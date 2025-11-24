package com.firebolt.kafka.connect;

import java.util.Arrays;

/**
 * As of Nov 2025 we support 2 types of ingestions:
 *  - sql - will insert data into firebolt using prepared statements
 *  - binary - will insert data into firebolt using a parquet file
 */
public enum IngestionType {

    SQL("sql"),
    BINARY("binary");

    private String value;

    IngestionType(String value) {
        this.value = value;
    }

    public static IngestionType fromValue(String value) {
        return Arrays.stream(IngestionType.values())
                .filter(ingestionType -> ingestionType.value.equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no ingestion type with value: " + value));
    }
}
