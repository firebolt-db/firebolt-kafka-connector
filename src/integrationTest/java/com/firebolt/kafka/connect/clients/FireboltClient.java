package com.firebolt.kafka.connect.clients;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.kafka.connect.utils.JdbcConnectionParser;
import org.apache.commons.lang3.StringUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Client class for Firebolt database operations used in integration tests.
 * This class centralizes all database operations like creating databases, tables,
 * and querying data to avoid code duplication across test classes.
 * 
 * The client maintains a persistent connection that should be closed when done.
 * 
 * Connection configuration can be customized using system properties:
 * - firebolt.url: Complete JDBC URL (overrides other properties)
 * - firebolt.host: Hostname (default: localhost)
 * - firebolt.port: Port number (default: 3473)
 */
@Slf4j
public class FireboltClient implements AutoCloseable {
    
    private static final String JDBC_CONNECTION_URL_FORMAT = "jdbc:firebolt:%s?url=http://localhost:3473";

    private final String jdbcUrl;
    private final Connection connection;
    
    /**
     * Creates a new FireboltClient with the specified JDBC URL.
     * Opens and maintains a persistent connection.
     * 
     * @param jdbcUrl the Firebolt JDBC connection URL
     * @throws SQLException if connection fails
     */
    private FireboltClient(String jdbcUrl) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.connection = DriverManager.getConnection(jdbcUrl);
    }

    private FireboltClient(String jdbcUrl, String clientId, String clientSecret) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        Properties props = new Properties();
        props.put("client_id", clientId);
        props.put("client_secret", clientSecret);

        this.connection = DriverManager.getConnection(jdbcUrl, props);
    }

    public static FireboltClient createDefault() throws SQLException {
        return createFor("");
    }

    public static FireboltClient createFor(String database) throws SQLException {
        // Check if clientId/clientSecret is explicitly configured
        String clientId = System.getProperty("clientId", null);
        String clientSecret = System.getProperty("clientSecret", null);

        if (StringUtils.isNotEmpty(clientId) && StringUtils.isNotEmpty(clientSecret)) {
            log.info("Using firebolt cloud");

            String jdbcUrl = System.getProperty("jdbc.connection.url");
            String databaseName = JdbcConnectionParser.getDatabase(jdbcUrl);

            return new FireboltClient(jdbcUrl.replaceFirst(databaseName, database), clientId, clientSecret);
        }

        // Build URL for core url
        log.info("Using firebolt core");
        String url = String.format(JDBC_CONNECTION_URL_FORMAT, database);
        return new FireboltClient(url);
    }

    /**
     * Closes the persistent database connection.
     * Should be called when the client is no longer needed.
     */
    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            log.debug("FireboltClient connection closed");
        }
    }

    /**
     * Tests the database connection.
     * 
     * @return true if connection is successful
     */
    public boolean testConnection() {
        try {
            return !connection.isClosed() && connection.createStatement().executeQuery("SELECT 1").next();
        } catch (SQLException e) {
            log.debug("Connection test failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Creates a database if it doesn't already exist.
     * 
     * @param databaseName the name of the database to create
     * @throws SQLException if database creation fails
     */
    public void createDatabase(String databaseName) throws SQLException {
        log.info("Creating database: {}", databaseName);
        
        try (Statement stmt = connection.createStatement()) {
            String createSql = "CREATE DATABASE IF NOT EXISTS \"" + databaseName + "\"";
            stmt.executeUpdate(createSql);
            log.info("✅ Created database: {}", databaseName);
        }
    }
    
    /**
     * Drops a database if it exists.
     * 
     * @param databaseName the name of the database to drop
     * @throws SQLException if database drop fails
     */
    public void dropDatabase(String databaseName) throws SQLException {
        log.info("Dropping database: {}", databaseName);
        
        try (Statement stmt = connection.createStatement()) {
            String dropSql = "DROP DATABASE IF EXISTS \"" + databaseName + "\"";
            stmt.executeUpdate(dropSql);
            log.info("✅ Dropped database: {}", databaseName);
        }
    }

    /**
     * Creates a table with the specified schema.
     * 
     * @param tableName the name of the table to create
     * @param schema the SQL schema definition (columns, constraints, etc.)
     * @throws SQLException if table creation fails
     */
    public void createTable(String tableName, String schema) throws SQLException {
        log.info("Creating table: {}", tableName);
        
        try (Statement stmt = connection.createStatement()) {
            String createSql = "CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (" + schema + ")";
            stmt.executeUpdate(createSql);
            log.info("✅ Created table: {}", tableName);
        }
    }

    /**
     * Creates a table with the specified full schema that includes the table name.
     *
     * @param createSchemaSql the SQL schema definition (columns, constraints, etc.)
     * @throws SQLException if table creation fails
     */
    public void createTable(String createSchemaSql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createSchemaSql);
        }
    }

    /**
     * Drops a table if it exists.
     * 
     * @param tableName the name of the table to drop
     * @throws SQLException if table drop fails
     */
    public void dropTable(String tableName) throws SQLException {
        log.info("Dropping table: {}", tableName);
        
        try (Statement stmt = connection.createStatement()) {
            String dropSql = "DROP TABLE IF EXISTS \"" + tableName + "\"";
            stmt.executeUpdate(dropSql);
            log.info("✅ Dropped table: {}", tableName);
        }
    }

    /**
     * Counts the number of rows in a table.
     * 
     * @param tableName the name of the table to count
     * @return the number of rows in the table
     * @throws SQLException if query fails
     */
    public int countRows(String tableName) throws SQLException {
        log.debug("Counting rows in table: {}", tableName);
        
        try (Statement stmt = connection.createStatement()) {
            String countSql = "SELECT COUNT(*) FROM \"" + tableName + "\"";
            try (ResultSet rs = stmt.executeQuery(countSql)) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    log.debug("Table '{}' has {} rows", tableName, count);
                    return count;
                }
                return 0;
            }
        }
    }
    
    /**
     * Executes a custom SQL query and returns the result set.
     * Note: The caller is responsible for closing the result set and statement.
     * 
     * @param sql the SQL query to execute
     * @return the result set
     * @throws SQLException if query fails
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Executing query: {}", sql);
        
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(sql);
    }
    
    /**
     * Executes a custom SQL update (INSERT, UPDATE, DELETE) and returns the number of affected rows.
     * 
     * @param sql the SQL update statement to execute
     * @return the number of affected rows
     * @throws SQLException if update fails
     */
    public int executeUpdate(String sql) throws SQLException {
        log.debug("Executing update: {}", sql);
        
        try (Statement stmt = connection.createStatement()) {
            int affectedRows = stmt.executeUpdate(sql);
            log.debug("Update affected {} rows", affectedRows);
            return affectedRows;
        }
    }
    
    /**
     * Creates a standard test table with commonly used columns.
     * This is a convenience method for integration tests.
     * 
     * @param tableName the name of the table to create
     * @throws SQLException if table creation fails
     */
    public void createStandardTestTable(String tableName) throws SQLException {
        String schema = "id INTEGER NOT NULL, " +
                       "topic TEXT, " +
                       "partition_num INTEGER, " +
                       "offset_num BIGINT, " +
                       "message TEXT, " +
                       "level TEXT, " +
                       "timestamp_ms BIGINT, " +
                       "created_at TIMESTAMP DEFAULT NOW()";
        
        createTable(tableName, schema);
    }

    public String getEngineUrl() {
        // parse the jdbc url to get the account name
        FireboltConnection fireboltConnection = (FireboltConnection) connection;
        return fireboltConnection.getSessionProperties().getHost();
    }
    
}