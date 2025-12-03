package com.firebolt.kafka.connect.ingestion.binary.parquet;

public class AvroNameSanitizer {

    /**
     * Avro record and field names must match [A-Za-z_][A-Za-z0-9_]*.
     * This helper converts any unsupported character to underscore and ensures a valid starting character.
     */
    public String toValidAvroName(String candidate) {
        if (candidate == null || candidate.isEmpty()) {
            return "record";
        }
        String sanitized = candidate.replaceAll("[^A-Za-z0-9_]", "_");
        // If everything became underscores, fall back to a safe prefix
        if (sanitized.chars().allMatch(ch -> ch == '_')) {
            return "record";
        }
        char first = sanitized.charAt(0);
        if (!((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') || first == '_')) {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

}
