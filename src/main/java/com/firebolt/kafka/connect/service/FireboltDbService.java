package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import com.firebolt.shadow.org.apache.commons.lang3.StringUtils;
import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for handling all database operations with Firebolt.
 * This service provides methods for schema discovery, table validation, and connection management.
 */
@Slf4j
public class FireboltDbService {

    // JDBC property names
    private static final String JDBC_CLIENT_ID = "client_id";
    private static final String JDBC_CLIENT_SECRET = "client_secret";

    /**
     * Discovers schemas for all specified tables.
     *
     * @param jdbcConfig the JDBC connection configuration
     * @param tableNames the set of table names to discover schemas for
     * @return a map of table names to their corresponding schemas
     * @throws ConnectionFailedException if there's an error connecting to the database
     */
    public Map<String, TableSchema> discoverTableSchemas(JdbcConfig jdbcConfig, Set<String> tableNames) throws ConnectionFailedException {
        if (StringUtils.isEmpty(jdbcConfig.getJdbcConnectionUrl())) {
            throw new ConnectionFailedException("Connection URL cannot be null or empty");
        }

        if (tableNames == null || tableNames.isEmpty()) {
            log.warn("There is no table names mapping to be resolved.");
            return new HashMap<>();
        }

        Map<String, TableSchema> tableSchemas = new HashMap<>();

        log.info("Discovering schemas for {} unique tables: {}", tableNames.size(), tableNames);

        try (Connection connection = getConnection(jdbcConfig)) {
            for (String tableName : tableNames) {
                try {
                    TableSchema schema = discoverTableSchema(connection, tableName);
                    if (schema != null) {
                        tableSchemas.put(tableName, schema);
                        log.info("Discovered schema for table '{}': {} columns",
                                tableName, schema.getColumns().size());
                    } else {
                        log.warn("Table '{}' not found in database", tableName);
                    }
                } catch (Exception e) {
                    log.error("Failed to discover schema for table '{}'", tableName, e);
                    throw new ConnectionFailedException("Failed to discover schema for table: " + tableName, e);
                }
            }

            log.info("Successfully discovered schemas for {} tables", tableSchemas.size());

        } catch (SQLException e) {
            log.error("Failed to connect to Firebolt for schema discovery", e);
            throw new ConnectionFailedException("Failed to connect to Firebolt for schema discovery", e);
        }

        return tableSchemas;
    }

    private TableSchema discoverTableSchema(Connection connection, String tableName) throws SQLException {
        log.debug("Discovering schema for table '{}'", tableName);

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getColumns(null, "public", tableName, null)) {
            TableSchema schema = new TableSchema(tableName);
            boolean hasColumns = false;

            while (rs.next()) {
                hasColumns = true;
                String columnName = rs.getString("COLUMN_NAME");
                String dataType = rs.getString("TYPE_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                boolean nullable = rs.getBoolean("NULLABLE");

                schema.addColumn(columnName, dataType, sqlType, nullable);
                log.debug("Found column in table '{}': {} ({}, SQL type: {}, nullable: {})",
                        tableName, columnName, dataType, sqlType, nullable);
            }

            return hasColumns ? schema : null;
        }
    }

    /**

    /**
     * Validates that all specified tables exist in the database.
     *
     * @param jdbcConfig the JDBC configuration
     * @param tableNames the set of table names to check
     * @return set of table names that do not exist in the database
     * @throws ConnectionFailedException if the database connection or query fails
     */
    public Set<String> validateTablesExist(JdbcConfig jdbcConfig, Set<String> tableNames) throws ConnectionFailedException {
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