package com.firebolt.kafka.connect.clients;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
public class FireboltClient implements AutoCloseable {
    
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FireboltClient.class);

    private static final String JDBC_CONNECTION_URL_FORMAT = "jdbc:firebolt:%s?url=http://%s:%s";

    private final String jdbcUrl;
    private final Connection connection;
    
    /**
     * Creates a new FireboltClient with the specified JDBC URL.
     * Opens and maintains a persistent connection.
     * 
     * @param jdbcUrl the Firebolt JDBC connection URL
     * @throws SQLException if connection fails
     */
    public FireboltClient(String jdbcUrl) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.connection = DriverManager.getConnection(jdbcUrl);
        log.debug("FireboltClient connected to: {}", jdbcUrl.replaceAll("password=[^&]*", "password=***"));
    }

    /**
     * Creates a new FireboltClient using default connection settings.
     * Uses environment variables or default ports for flexibility.
     *
     * @return a new FireboltClient instance
     * @throws SQLException if connection fails
     */
    public static FireboltClient createDefault() throws SQLException {
        return createFor("");
    }

    /**
     * Creates a new FireboltClient for a particular database.
     * Uses environment variables or default ports for flexibility.
     *
     * @param database the database name (empty string for default)
     * @return a new FireboltClient instance
     * @throws SQLException if connection fails
     */
    public static FireboltClient createFor(String database) throws SQLException {
        // Check if URL is explicitly configured
        String explicitUrl = System.getProperty("firebolt.url");
        if (explicitUrl != null && !explicitUrl.isEmpty()) {
            log.debug("Using explicit Firebolt URL from system property: {}", explicitUrl);
            return new FireboltClient(explicitUrl);
        }
        
        // Build URL using system properties
        String host = System.getProperty("firebolt.host", "localhost");
        String port = System.getProperty("firebolt.port", "3473");
        String url = String.format(JDBC_CONNECTION_URL_FORMAT, database, host, port);
        
        log.debug("Using Firebolt connection: {}:{}", host, port);
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
            String createSql = "CREATE DATABASE IF NOT EXISTS " + databaseName;
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
            String dropSql = "DROP DATABASE IF EXISTS " + databaseName;
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
            String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + schema + ")";
            stmt.executeUpdate(createSql);
            log.info("✅ Created table: {}", tableName);
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
            String dropSql = "DROP TABLE IF EXISTS " + tableName;
            stmt.executeUpdate(dropSql);
            log.info("✅ Dropped table: {}", tableName);
        }
    }
    
    /**
     * Checks if a table exists in the database.
     * 
     * @param tableName the name of the table to check
     * @return true if the table exists
     * @throws SQLException if query fails
     */
    public boolean tableExists(String tableName) throws SQLException {
        log.debug("Checking if table exists: {}", tableName);
        
        String query = "SELECT 1 FROM information_schema.tables " +
                      "WHERE table_schema = 'public' AND LOWER(table_name) = LOWER(?) LIMIT 1";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                boolean exists = rs.next();
                log.debug("Table '{}' exists: {}", tableName, exists);
                return exists;
            }
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
            String countSql = "SELECT COUNT(*) FROM " + tableName;
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
    
    /**
     * Creates a comprehensive test table with all Firebolt data types.
     * This includes all numeric, boolean, composite, date/timestamp, string, binary, and spatial types.
     * Useful for testing data type compatibility and conversion.
     * 
     * @param tableName the name of the table to create
     * @throws SQLException if table creation fails
     */
    public void createAllDataTypesTestTable(String tableName) throws SQLException {
        String schema = 
            // Numeric types
            "\"colInteger\" INTEGER NOT NULL, " +
            "\"colBigint\" BIGINT, " +
            "\"colNumeric\" NUMERIC(38,9), " +
            "\"colReal\" REAL, " +
            "\"colDoublePrecision\" DOUBLE PRECISION, " +

            // Boolean type
            "\"colBoolean\" BOOLEAN, " +

            // String type
            "\"colText\" TEXT, " +
            
            // Date and timestamp types
            "\"colDate\" DATE, " +
            "\"colTimestamp\" TIMESTAMP, " +
            "\"colTimestamptz\" TIMESTAMPTZ, " +
            
            // Binary type
            "\"colBytea\" BYTEA, " +
            
            // Array types (various syntaxes and element types)
            "\"colArrayTextNullable\" ARRAY(TEXT NULL), " +
            "\"colArrayTextNotNull\" ARRAY(TEXT NOT NULL), " +
            "\"colArrayIntSyntax1\" ARRAY(INTEGER), " +
            "\"colArrayIntSyntax2\" INTEGER[], " +
            "\"colArrayDate\" ARRAY(DATE), " +
            "\"colArrayReal\" ARRAY(REAL), " +
            "\"colArrayNested\" ARRAY(ARRAY(INTEGER)), " +
            "\"colArrayNumeric\" ARRAY(NUMERIC), " +
            "\"colArrayDoublePrecision\" ARRAY(DOUBLE PRECISION), " +
            "\"colArrayTimestamptz\" ARRAY(TIMESTAMPTZ), " +
            "\"colArrayTimestamp\" ARRAY(TIMESTAMP), " +
            
            // STRUCT type stored as JSON text (JDBC driver doesn't support STRUCT type)
            "\"colStruct\" TEXT" +
            
            // GEOGRAPHY type  
            "";
        
        createTable(tableName, schema);
    }
    
    /**
     * Creates a simple test table with 5 basic columns.
     * This is useful for basic integration testing with common data types.
     * 
     * @param tableName the name of the table to create
     * @throws SQLException if table creation fails
     */
    public void createSimpleTestTable(String tableName) throws SQLException {
        String schema = 
            "id BIGINT NOT NULL, " +
            "\"createdAt\" TIMESTAMPTZ, " + // use quotes to preserve case
            "\"recordTimestamp\" BIGINT, " +
            "title TEXT, " +
            "description TEXT";
        
        createTable(tableName, schema);
    }
    
}