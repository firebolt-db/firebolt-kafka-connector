package com.firebolt.kafka.connect.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Configuration options for database connection testing.
 */
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ConnectionOptions {

    private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 5;

    @Builder.Default
    private int connectionTimeoutSeconds = DEFAULT_CONNECTION_TIMEOUT_SECONDS;

}