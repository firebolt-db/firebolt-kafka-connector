package com.firebolt.kafka.connect.load;

/**
 * Supported message formats for load tests.
 */
public enum MessageType {
    JSON("json"),
    AVRO("avro");

    private final String value;

    MessageType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parses a string (e.g. from system property or workflow input) to MessageType.
     * Case-insensitive. Defaults to JSON if null or blank.
     */
    public static MessageType fromString(String s) {
        if (s == null || s.isBlank()) {
            return JSON;
        }
        for (MessageType t : values()) {
            if (t.value.equalsIgnoreCase(s)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unsupported message type: " + s + ". Use 'json' or 'avro'.");
    }
}
