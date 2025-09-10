package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

/**
 * Singleton provider for FireboltSinkService implementations.
 * Returns the appropriate service implementation based on the connector configuration.
 *
 * IMPORTANT: This provider creates a new service instance for every request to ensure
 * complete isolation between connectors and avoid any configuration sharing issues.
 */
@Slf4j
public class FireboltSinkServiceProvider {

    private static final FireboltSinkServiceProvider INSTANCE = new FireboltSinkServiceProvider();

    /**
     * Private constructor to enforce singleton pattern.
     */
    private FireboltSinkServiceProvider() {
    }

    /**
     * Gets the singleton instance of FireboltSinkServiceProvider.
     *
     * @return the singleton instance
     */
    public static FireboltSinkServiceProvider getInstance() {
        return INSTANCE;
    }

    /**
     * Returns a new FireboltSinkService implementation based on the configuration.
     * A new instance is created for every call to ensure complete isolation.
     *
     * @param sinkConfig the connector configuration properties
     * @return a new FireboltSinkService implementation
     * @throws IllegalArgumentException if the sink connector type is not supported
     */
    public FireboltSinkService getService(SinkConfig sinkConfig, ErrorReporter errorReporter, boolean errorToleranceAll) {
        if (sinkConfig == null) {
            throw new IllegalArgumentException("Configuration properties cannot be null");
        }
        return new AppendOnlyFireboltSinkService(sinkConfig, errorReporter, errorToleranceAll);
    }
}
