package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for handling all database operations with Firebolt.
 * This service provides methods for table validation and connection management.
 */
@Slf4j
public class FireboltDbService {

    // JDBC property names
    private static final String JDBC_CLIENT_ID = "client_id";
    private static final String JDBC_CLIENT_SECRET = "client_secret";

    /**
     * Returns the subset of the specified table names that do not exist in the database.
     *
     * @param jdbcConfig the JDBC configuration
     * @param tableNames the set of table names to check
     * @return set of table names that do not exist in the database
     * @throws ConnectionFailedException if the connection URL is missing
     */
    public Set<String> findNonExistentTables(JdbcConfig jdbcConfig, Set<String> tableNames) throws ConnectionFailedException {
        if (StringUtils.isBlank(jdbcConfig.getJdbcConnectionUrl())) {
            throw new ConnectionFailedException("Connection URL cannot be null or empty");
        }

        if (tableNames == null || tableNames.isEmpty()) {
            return new HashSet<>();
        }

        log.warn("Validating existence of {} tables: {}", tableNames.size(), tableNames);

        Set<String> allTables = getTableNames(jdbcConfig);
        Set<String> nonExistentTables = Sets.difference(tableNames, allTables);

        return nonExistentTables;
    }

    private Set<String> getTableNames(JdbcConfig jdbcConfig) {
        try (Connection connection = getConnection(jdbcConfig)) {
            Set<String> allTables = new HashSet<>();
            ResultSet tableResultSet = connection.getMetaData().getTables(connection.getCatalog(), connection.getSchema(), "%", new String[]{"TABLE"});

            while (tableResultSet.next()) {
                allTables.add(tableResultSet.getString("TABLE_NAME"));
                log.info(tableResultSet.getString("TABLE_NAME"));
            }
            return allTables;

        } catch (SQLException e) {
            log.error("Cannot determine the table names in firebolt");
            return Collections.emptySet();
        }
    }

    private Connection getConnection(JdbcConfig jdbcConfig) throws SQLException {
        if (jdbcConfig == null) {
            throw new SQLException("JdbcConfig cannot be null");
        }

        Properties props = new Properties();

        Optional<String> clientId = jdbcConfig.getClientId();
        if (clientId != null && clientId.isPresent() && !clientId.get().trim().isEmpty()) {
            props.setProperty(JDBC_CLIENT_ID, clientId.get());
        }

        Optional<String> clientSecret = jdbcConfig.getClientSecret();
        if (clientSecret != null && clientSecret.isPresent() && !clientSecret.get().trim().isEmpty()) {
            props.setProperty(JDBC_CLIENT_SECRET, clientSecret.get());
        }

        // always batch prepared statements
        props.put("merge_prepared_statement_batches", "true");
        props.put("compress_request_payload", "true");

        // Attempt to create the connection
        return DriverManager.getConnection(jdbcConfig.getJdbcConnectionUrl(), props);
    }

    /**
     * Tests the JDBC connection to Firebolt using the provided connection URL.
     *
     * @param jdbcConfig the JDBC connection configuration
     * @throws ConnectionFailedException if the connection fails for any reason
     */
    public void testConnection(JdbcConfig jdbcConfig) throws ConnectionFailedException {
        testConnection(jdbcConfig, ConnectionOptions.builder().build()); // Default timeout of 5 seconds
    }

    /**
     * Tests the JDBC connection to Firebolt using the provided connection URL.
     *
     * @param jdbcConfig the JDBC connection configuration
     * @param connectionOptions - timeout in seconds for connection validation
     * @throws ConnectionFailedException if the connection fails for any reason
     */
    public void testConnection(JdbcConfig jdbcConfig, ConnectionOptions connectionOptions) throws ConnectionFailedException {
        if (StringUtils.isBlank(jdbcConfig.getJdbcConnectionUrl())) {
            throw new ConnectionFailedException("Connection URL cannot be null or empty");
        }

        if (connectionOptions == null) {
            connectionOptions = ConnectionOptions.builder().build();
        }

        if (connectionOptions.getConnectionTimeoutSeconds() < 0) {
            throw new IllegalArgumentException("Timeout must be non-negative");
        }

        try {
            log.info("Testing connection to Firebolt database: {}", jdbcConfig);

            // Attempt to establish connection
            try (Connection connection = getConnection(jdbcConfig)) {
                if (connection.isValid(connectionOptions.getConnectionTimeoutSeconds())) {
                    log.info("Successfully connected to Firebolt database");
                } else {
                    throw new ConnectionFailedException("Connection is not valid");
                }
            }

        } catch (SQLException e) {
            throw new ConnectionFailedException("Failed to connect to Firebolt database: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new ConnectionFailedException("Unexpected error while testing connection to Firebolt", e);
        }
    }

    /**
     * Creates a new connection to the Firebolt database.
     *
     * @param jdbcConfig the JDBC connection configuration
     * @return a new database connection
     * @throws ConnectionFailedException if the connection fails
     */
    public Connection createConnection(JdbcConfig jdbcConfig) throws ConnectionFailedException {
        try {
            log.debug("Creating connection to Firebolt database");
            Connection connection = getConnection(jdbcConfig);

            // need to replace this with false as to manage the transactions in a layer above
            connection.setAutoCommit(true);
            return connection;
        } catch (SQLException e) {
            throw new ConnectionFailedException("Failed to create connection to Firebolt database: " + e.getMessage(), e);
        }
    }
}