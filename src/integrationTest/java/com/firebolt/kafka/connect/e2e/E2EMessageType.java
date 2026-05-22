package com.firebolt.kafka.connect.e2e;

/** Supported message formats for E2E tests. */
public enum E2EMessageType {
    JSON("json"),
    AVRO("avro"),
    PROTOBUF("protobuf");

    private final String value;

    E2EMessageType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
