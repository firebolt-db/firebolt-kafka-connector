package com.firebolt.kafka.connect.e2e;

/** Delivery semantics for E2E tests. */
public enum DeliveryMode {
    AT_LEAST_ONCE("at_least_once"),
    EXACTLY_ONCE("exactly_once");

    private final String value;

    DeliveryMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
