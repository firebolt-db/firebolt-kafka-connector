package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FireboltDbService.
 */
@ExtendWith(MockitoExtension.class)
class FireboltDbServiceTest {

    private FireboltDbService fireboltDbService;
    private JdbcConfig jdbcConfig;

    @BeforeEach
    void setUp() {
        fireboltDbService = new FireboltDbService();
        jdbcConfig = JdbcConfig.builder()
                .jdbcConnectionUrl("jdbc:firebolt:test_database?engine=test_engine&account=test_account")
                .clientId(Optional.of("test_client_id"))
                .clientSecret(Optional.of("test_client_secret"))
                .build();
    }
    
    /**
     * Helper method to create JdbcConfig from connection URL for tests
     */
    private JdbcConfig createJdbcConfig(String connectionUrl) {
        return JdbcConfig.builder()
                .jdbcConnectionUrl(connectionUrl)
                .clientId(Optional.empty())
                .clientSecret(Optional.empty())
                .build();
    }

    @Nested
    class TableValidationTests {

        @Test
        void shouldReturnEmptySetWhenAllTablesExist() throws SQLException {
            Set<String> tableNames = Set.of("table1", "table2");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                when(mockConnection.getCatalog()).thenReturn("test_catalog");
                when(mockConnection.getSchema()).thenReturn("test_schema");
                
                when(mockMetaData.getTables(eq("test_catalog"), eq("test_schema"), eq("%"), eq(new String[]{"TABLE"})))
                        .thenReturn(mockResultSet);
                
                when(mockResultSet.next()).thenReturn(true, true, false);
                when(mockResultSet.getString("TABLE_NAME")).thenReturn("table1", "table2");

                Set<String> result = fireboltDbService.validateTablesExist(jdbcConfig, tableNames);

                assertTrue(result.isEmpty(), "Should return empty set when all tables exist");
            }
        }

        @Test
        void shouldReturnNonExistentTables() throws SQLException {
            Set<String> tableNames = Set.of("table1", "table2", "table3");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                when(mockConnection.getCatalog()).thenReturn("test_catalog");
                when(mockConnection.getSchema()).thenReturn("test_schema");
                
                when(mockMetaData.getTables(anyString(), anyString(), anyString(), any(String[].class)))
                        .thenReturn(mockResultSet);
                
                when(mockResultSet.next()).thenReturn(true, false);
                when(mockResultSet.getString("TABLE_NAME")).thenReturn("table1");

                Set<String> result = fireboltDbService.validateTablesExist(jdbcConfig, tableNames);

                assertEquals(2, result.size(), "Should return 2 non-existent tables");
                assertTrue(result.contains("table2"), "Should contain table2");
                assertTrue(result.contains("table3"), "Should contain table3");
                assertFalse(result.contains("table1"), "Should not contain table1 (exists)");
            }
        }

        @Test
        void shouldReturnEmptySetWhenTableNamesNull() throws ConnectionFailedException {
            Set<String> result = fireboltDbService.validateTablesExist(jdbcConfig, null);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void shouldReturnEmptySetWhenTableNamesEmpty() throws ConnectionFailedException {
            Set<String> result = fireboltDbService.validateTablesExist(jdbcConfig, Collections.emptySet());

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void shouldThrowExceptionWhenJdbcUrlNull() {
            JdbcConfig configWithNullUrl = JdbcConfig.builder()
                    .jdbcConnectionUrl(null)
                    .clientId(Optional.of("test_client_id"))
                    .clientSecret(Optional.of("test_client_secret"))
                    .build();

            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.validateTablesExist(configWithNullUrl, Set.of("table1")));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldThrowExceptionWhenJdbcUrlEmpty() {
            JdbcConfig configWithEmptyUrl = JdbcConfig.builder()
                    .jdbcConnectionUrl("")
                    .clientId(Optional.of("test_client_id"))
                    .clientSecret(Optional.of("test_client_secret"))
                    .build();

            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.validateTablesExist(configWithEmptyUrl, Set.of("table1")));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldHandleDatabaseConnectionFailuresGracefully() {
            JdbcConfig invalidConfig = JdbcConfig.builder()
                    .jdbcConnectionUrl("jdbc:firebolt:invalid://invalid:123/invalid")
                    .clientId(Optional.of("invalid"))
                    .clientSecret(Optional.of("invalid"))
                    .build();
            Set<String> tableNames = Set.of("table1", "table2");

            Set<String> result = fireboltDbService.validateTablesExist(invalidConfig, tableNames);

            assertEquals(tableNames, result, "Should return all table names when connection fails");
        }
    }

    @Nested
    class ConnectionTests {

        @Test
        void shouldSuccessfullyTestConnectionWithValidUrl() throws SQLException {
            String connectionUrl = "jdbc:firebolt:test_database?engine=test_engine&account=test_account";
            Connection mockConnection = mock(Connection.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenReturn(mockConnection);
                when(mockConnection.isValid(300)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl)));
                
                verify(mockConnection).isValid(300);
            }
        }

        @Test
        void shouldSuccessfullyTestConnectionWithCustomTimeout() throws SQLException {
            String connectionUrl = "jdbc:firebolt:test_database?engine=test_engine&account=test_account";
            int customTimeout = 10;
            ConnectionOptions connectionOptions = ConnectionOptions.builder()
                    .connectionTimeoutSeconds(customTimeout)
                    .build();
            Connection mockConnection = mock(Connection.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenReturn(mockConnection);
                when(mockConnection.isValid(customTimeout)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl), connectionOptions));
                
                verify(mockConnection).isValid(customTimeout);
            }
        }

        @Test
        void shouldThrowExceptionWhenConnectionUrlNull() {
            JdbcConfig nullUrlConfig = JdbcConfig.builder()
                    .jdbcConnectionUrl(null)
                    .clientId(Optional.empty())
                    .clientSecret(Optional.empty())
                    .build();
                    
            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.testConnection(nullUrlConfig));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldThrowExceptionWhenConnectionUrlEmpty() {
            JdbcConfig emptyUrlConfig = JdbcConfig.builder()
                    .jdbcConnectionUrl("")
                    .clientId(Optional.empty())
                    .clientSecret(Optional.empty())
                    .build();
                    
            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.testConnection(emptyUrlConfig));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldThrowExceptionWhenConnectionUrlWhitespace() {
            JdbcConfig whitespaceUrlConfig = JdbcConfig.builder()
                    .jdbcConnectionUrl("   ")
                    .clientId(Optional.empty())
                    .clientSecret(Optional.empty())
                    .build();
                    
            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.testConnection(whitespaceUrlConfig));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldThrowExceptionWhenTimeoutNegative() {
            String connectionUrl = "jdbc:firebolt:test_database";
            ConnectionOptions invalidOptions = ConnectionOptions.builder()
                    .connectionTimeoutSeconds(-1)
                    .build();

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl), invalidOptions));
            
            assertEquals("Timeout must be non-negative", exception.getMessage());
        }

        @Test
        void shouldThrowConnectionFailedExceptionWhenSqlExceptionOccurs() throws SQLException {
            String connectionUrl = "jdbc:firebolt:invalid_database";

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                SQLException sqlException = new SQLException("Invalid connection string");
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenThrow(sqlException);

                ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                        () -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl)));
                
                assertTrue(exception.getMessage().contains("Failed to connect to Firebolt database"));
                assertTrue(exception.getMessage().contains("Invalid connection string"));
                assertEquals(sqlException, exception.getCause());
            }
        }

        @Test
        void shouldThrowConnectionFailedExceptionForInvalidConnection() {
            String invalidConnectionUrl = "jdbc:firebolt:invalid://invalid:123/invalid";

            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.testConnection(createJdbcConfig(invalidConnectionUrl)));
            
            assertTrue(exception.getMessage().contains("Failed to connect to Firebolt database"));
        }

        @Test
        void shouldThrowConnectionFailedExceptionForUnexpectedExceptions() throws SQLException {
            String connectionUrl = "jdbc:firebolt:test_database";

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenThrow(new RuntimeException("Unexpected error"));

                ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                        () -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl)));
                
                assertTrue(exception.getMessage().contains("Unexpected error while testing connection"));
            }
        }

        @Test
        void shouldUseDefaultConnectionOptionsWhenNoneProvided() throws SQLException {
            String connectionUrl = "jdbc:firebolt:test_database?engine=test_engine&account=test_account";
            Connection mockConnection = mock(Connection.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenReturn(mockConnection);
                when(mockConnection.isValid(300)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl)));
                
                verify(mockConnection).isValid(300);
            }
        }

        @Test
        void shouldHandleZeroTimeoutCorrectly() throws SQLException {
            String connectionUrl = "jdbc:firebolt:test_database";
            ConnectionOptions zeroTimeoutOptions = ConnectionOptions.builder()
                    .connectionTimeoutSeconds(0)
                    .build();
            Connection mockConnection = mock(Connection.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenReturn(mockConnection);
                when(mockConnection.isValid(0)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl), zeroTimeoutOptions));
                
                verify(mockConnection).isValid(0);
            }
        }

        @Test
        void shouldHandleLargeTimeoutValues() throws SQLException {
            String connectionUrl = "jdbc:firebolt:test_database";
            int largeTimeout = 300;
            ConnectionOptions largeTimeoutOptions = ConnectionOptions.builder()
                    .connectionTimeoutSeconds(largeTimeout)
                    .build();
            Connection mockConnection = mock(Connection.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(eq(connectionUrl), any(Properties.class)))
                        .thenReturn(mockConnection);
                when(mockConnection.isValid(largeTimeout)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl), largeTimeoutOptions));
                
                verify(mockConnection).isValid(largeTimeout);
            }
        }
    }

    @Nested
    class ConnectionOptionsTests {

        @Test
        void shouldCreateConnectionOptionsWithDefaultTimeout() {
            ConnectionOptions options = ConnectionOptions.builder().build();

            assertEquals(300, options.getConnectionTimeoutSeconds(), "Default timeout should be 300 seconds");
        }

        @Test
        void shouldCreateConnectionOptionsWithCustomTimeout() {
            ConnectionOptions options = ConnectionOptions.builder()
                    .connectionTimeoutSeconds(30)
                    .build();

            assertEquals(30, options.getConnectionTimeoutSeconds(), "Custom timeout should be 30 seconds");
        }

        @Test
        void shouldCreateConnectionOptionsWithNoArgsConstructor() {
            ConnectionOptions options = new ConnectionOptions();

            assertEquals(300, options.getConnectionTimeoutSeconds(), "Default timeout should be 300 seconds");
        }

        @Test
        void shouldCreateConnectionOptionsWithAllArgsConstructor() {
            ConnectionOptions options = new ConnectionOptions(60);

            assertEquals(60, options.getConnectionTimeoutSeconds(), "Timeout should be 60 seconds");
        }

        @Test
        void shouldHaveMeaningfulToStringRepresentation() {
            ConnectionOptions options = ConnectionOptions.builder()
                    .connectionTimeoutSeconds(15)
                    .build();

            String toString = options.toString();

            assertNotNull(toString);
            assertTrue(toString.contains("15"), "toString should contain the timeout value");
        }
    }

    @Nested
    class JdbcConfigurationTests {

        @Test
        void shouldHandleJdbcConfigWithClientCredentials() throws SQLException {
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                when(mockConnection.getCatalog()).thenReturn("test_catalog");
                when(mockConnection.getSchema()).thenReturn("test_schema");
                when(mockMetaData.getTables(anyString(), anyString(), anyString(), any(String[].class)))
                        .thenReturn(mockResultSet);
                when(mockResultSet.next()).thenReturn(false);

                fireboltDbService.validateTablesExist(jdbcConfig, Set.of("table1"));

                mockedDriverManager.verify(() -> DriverManager.getConnection(
                        eq("jdbc:firebolt:test_database?engine=test_engine&account=test_account"),
                        argThat(props -> {
                            Properties properties = (Properties) props;
                            return "test_client_id".equals(properties.getProperty("client_id")) &&
                                   "test_client_secret".equals(properties.getProperty("client_secret"));
                        })
                ));
            }
        }

        @Test
        void shouldHandleJdbcConfigWithoutClientCredentials() throws SQLException {
            JdbcConfig configWithoutCredentials = JdbcConfig.builder()
                    .jdbcConnectionUrl("jdbc:firebolt:test_database")
                    .clientId(Optional.empty())
                    .clientSecret(Optional.empty())
                    .build();

            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                when(mockConnection.getCatalog()).thenReturn("test_catalog");
                when(mockConnection.getSchema()).thenReturn("test_schema");
                when(mockMetaData.getTables(anyString(), anyString(), anyString(), any(String[].class)))
                        .thenReturn(mockResultSet);
                when(mockResultSet.next()).thenReturn(false);

                fireboltDbService.validateTablesExist(configWithoutCredentials, Set.of("table1"));

                mockedDriverManager.verify(() -> DriverManager.getConnection(
                        eq("jdbc:firebolt:test_database"),
                        argThat(props -> {
                            Properties properties = (Properties) props;
                            return !properties.containsKey("client_id") &&
                                   !properties.containsKey("client_secret");
                        })
                ));
            }
        }

        @Test
        void shouldHandleJdbcConfigWithEmptyClientCredentials() throws SQLException {
            JdbcConfig configWithEmptyCredentials = JdbcConfig.builder()
                    .jdbcConnectionUrl("jdbc:firebolt:test_database")
                    .clientId(Optional.of(""))
                    .clientSecret(Optional.of("   "))
                    .build();

            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                when(mockConnection.getCatalog()).thenReturn("test_catalog");
                when(mockConnection.getSchema()).thenReturn("test_schema");
                when(mockMetaData.getTables(anyString(), anyString(), anyString(), any(String[].class)))
                        .thenReturn(mockResultSet);
                when(mockResultSet.next()).thenReturn(false);

                fireboltDbService.validateTablesExist(configWithEmptyCredentials, Set.of("table1"));

                mockedDriverManager.verify(() -> DriverManager.getConnection(
                        eq("jdbc:firebolt:test_database"),
                        argThat(props -> {
                            Properties properties = (Properties) props;
                            return !properties.containsKey("client_id") &&
                                   !properties.containsKey("client_secret");
                        })
                ));
            }
        }
    }

    @Nested
    class SchemaDiscoveryTests {

        @Test
        void shouldDiscoverSchemaForSingleTable() throws SQLException {
            Set<String> tableNames = Set.of("test_table");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockColumnResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("test_table"), eq(null)))
                        .thenReturn(mockColumnResultSet);
                
                // Mock column data
                when(mockColumnResultSet.next())
                        .thenReturn(true, true, false); // Two columns, then end
                when(mockColumnResultSet.getString("COLUMN_NAME"))
                        .thenReturn("id", "name");
                when(mockColumnResultSet.getString("TYPE_NAME"))
                        .thenReturn("INTEGER", "TEXT");
                when(mockColumnResultSet.getInt("DATA_TYPE"))
                        .thenReturn(4, 12); // SQL types for INTEGER and TEXT
                when(mockColumnResultSet.getBoolean("NULLABLE"))
                        .thenReturn(false, true);

                Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                    fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                assertNotNull(result);
                assertEquals(1, result.size());
                assertTrue(result.containsKey("test_table"));
                
                com.firebolt.kafka.connect.TableSchema schema = result.get("test_table");
                assertEquals("test_table", schema.getTableName());
                assertEquals(2, schema.getColumns().size());
                
                // Verify first column
                com.firebolt.kafka.connect.TableSchema.Column column1 = schema.getColumns().get(0);
                assertEquals("id", column1.getName());
                assertEquals("INTEGER", column1.getDataType());
                assertEquals(4, column1.getSqlType());
                assertFalse(column1.isNullable());
                
                // Verify second column
                com.firebolt.kafka.connect.TableSchema.Column column2 = schema.getColumns().get(1);
                assertEquals("name", column2.getName());
                assertEquals("TEXT", column2.getDataType());
                assertEquals(12, column2.getSqlType());
                assertTrue(column2.isNullable());
            }
        }

        @Test
        void shouldDiscoverSchemasForMultipleTables() throws SQLException {
            Set<String> tableNames = Set.of("table1", "table2");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockTable1ResultSet = mock(ResultSet.class);
            ResultSet mockTable2ResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                
                // Mock table1 columns
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("table1"), eq(null)))
                        .thenReturn(mockTable1ResultSet);
                when(mockTable1ResultSet.next()).thenReturn(true, false);
                when(mockTable1ResultSet.getString("COLUMN_NAME")).thenReturn("id");
                when(mockTable1ResultSet.getString("TYPE_NAME")).thenReturn("BIGINT");
                when(mockTable1ResultSet.getInt("DATA_TYPE")).thenReturn(-5);
                when(mockTable1ResultSet.getBoolean("NULLABLE")).thenReturn(false);
                
                // Mock table2 columns
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("table2"), eq(null)))
                        .thenReturn(mockTable2ResultSet);
                when(mockTable2ResultSet.next()).thenReturn(true, true, false);
                when(mockTable2ResultSet.getString("COLUMN_NAME")).thenReturn("user_id", "email");
                when(mockTable2ResultSet.getString("TYPE_NAME")).thenReturn("INTEGER", "VARCHAR");
                when(mockTable2ResultSet.getInt("DATA_TYPE")).thenReturn(4, 12);
                when(mockTable2ResultSet.getBoolean("NULLABLE")).thenReturn(false, true);

                Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                    fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                assertNotNull(result);
                assertEquals(2, result.size());
                assertTrue(result.containsKey("table1"));
                assertTrue(result.containsKey("table2"));
                
                // Verify table1 schema
                com.firebolt.kafka.connect.TableSchema table1Schema = result.get("table1");
                assertEquals("table1", table1Schema.getTableName());
                assertEquals(1, table1Schema.getColumns().size());
                assertEquals("id", table1Schema.getColumns().get(0).getName());
                
                // Verify table2 schema
                com.firebolt.kafka.connect.TableSchema table2Schema = result.get("table2");
                assertEquals("table2", table2Schema.getTableName());
                assertEquals(2, table2Schema.getColumns().size());
                assertEquals("user_id", table2Schema.getColumns().get(0).getName());
                assertEquals("email", table2Schema.getColumns().get(1).getName());
            }
        }

        @Test
        void shouldReturnEmptyMapWhenNoTableNamesProvided() throws ConnectionFailedException {
            Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                fireboltDbService.discoverTableSchemas(jdbcConfig, Collections.emptySet());

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void shouldReturnEmptyMapWhenTableNamesIsNull() throws ConnectionFailedException {
            Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                fireboltDbService.discoverTableSchemas(jdbcConfig, null);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void shouldThrowExceptionWhenConnectionUrlIsNull() {
            JdbcConfig configWithNullUrl = JdbcConfig.builder()
                    .jdbcConnectionUrl(null)
                    .clientId(Optional.of("test_client_id"))
                    .clientSecret(Optional.of("test_client_secret"))
                    .build();

            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.discoverTableSchemas(configWithNullUrl, Set.of("table1")));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldThrowExceptionWhenConnectionUrlIsEmpty() {
            JdbcConfig configWithEmptyUrl = JdbcConfig.builder()
                    .jdbcConnectionUrl("")
                    .clientId(Optional.of("test_client_id"))
                    .clientSecret(Optional.of("test_client_secret"))
                    .build();

            ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                    () -> fireboltDbService.discoverTableSchemas(configWithEmptyUrl, Set.of("table1")));
            
            assertEquals("Connection URL cannot be null or empty", exception.getMessage());
        }

        @Test
        void shouldThrowExceptionWhenConnectionFails() throws SQLException {
            Set<String> tableNames = Set.of("test_table");

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                SQLException sqlException = new SQLException("Connection failed");
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenThrow(sqlException);

                ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                        () -> fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames));
                
                assertTrue(exception.getMessage().contains("Failed to connect to Firebolt for schema discovery"));
                assertEquals(sqlException, exception.getCause());
            }
        }

        @Test
        void shouldThrowExceptionWhenSchemaDiscoveryFails() throws SQLException {
            Set<String> tableNames = Set.of("test_table");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                SQLException sqlException = new SQLException("Schema discovery failed");
                when(mockMetaData.getColumns(anyString(), anyString(), anyString(), anyString()))
                        .thenThrow(sqlException);

                ConnectionFailedException exception = assertThrows(ConnectionFailedException.class,
                        () -> fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames));
                
                assertTrue(exception.getMessage().contains("Failed to discover schema for table: test_table"));
                assertNotNull(exception.getCause()); // Just verify there is a cause
            }
        }

        @Test
        void shouldSkipTableWhenNoColumnsFound() throws SQLException {
            Set<String> tableNames = Set.of("empty_table");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("empty_table"), eq(null)))
                        .thenReturn(mockResultSet);
                when(mockResultSet.next()).thenReturn(false); // No columns found

                Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                    fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                assertNotNull(result);
                assertTrue(result.isEmpty()); // Table should not be included if no columns found
            }
        }

        @Test
        void shouldDiscoverSchemaWithVariousColumnTypes() throws SQLException {
            Set<String> tableNames = Set.of("complex_table");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("complex_table"), eq(null)))
                        .thenReturn(mockResultSet);
                
                // Mock multiple column types
                when(mockResultSet.next())
                        .thenReturn(true, true, true, true, true, false);
                when(mockResultSet.getString("COLUMN_NAME"))
                        .thenReturn("id", "name", "created_at", "is_active", "balance");
                when(mockResultSet.getString("TYPE_NAME"))
                        .thenReturn("BIGINT", "TEXT", "TIMESTAMP", "BOOLEAN", "NUMERIC");
                when(mockResultSet.getInt("DATA_TYPE"))
                        .thenReturn(-5, 12, 93, 16, 3); // SQL types
                when(mockResultSet.getBoolean("NULLABLE"))
                        .thenReturn(false, true, false, true, true);

                Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                    fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                assertNotNull(result);
                assertEquals(1, result.size());
                
                com.firebolt.kafka.connect.TableSchema schema = result.get("complex_table");
                assertEquals("complex_table", schema.getTableName());
                assertEquals(5, schema.getColumns().size());
                
                // Verify all column types are correctly discovered
                List<com.firebolt.kafka.connect.TableSchema.Column> columns = schema.getColumns();
                assertEquals("id", columns.get(0).getName());
                assertEquals("BIGINT", columns.get(0).getDataType());
                assertEquals(-5, columns.get(0).getSqlType());
                assertFalse(columns.get(0).isNullable());
                
                assertEquals("name", columns.get(1).getName());
                assertEquals("TEXT", columns.get(1).getDataType());
                assertTrue(columns.get(1).isNullable());
                
                assertEquals("created_at", columns.get(2).getName());
                assertEquals("TIMESTAMP", columns.get(2).getDataType());
                assertFalse(columns.get(2).isNullable());
                
                assertEquals("is_active", columns.get(3).getName());
                assertEquals("BOOLEAN", columns.get(3).getDataType());
                assertTrue(columns.get(3).isNullable());
                
                assertEquals("balance", columns.get(4).getName());
                assertEquals("NUMERIC", columns.get(4).getDataType());
                assertTrue(columns.get(4).isNullable());
            }
        }

        @Test
        void shouldHandlePartialSuccessWhenSomeTablesExistAndOthersDont() throws SQLException {
            Set<String> tableNames = Set.of("existing_table", "nonexistent_table");
            
            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockExistingTableResultSet = mock(ResultSet.class);
            ResultSet mockNonexistentTableResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);
                
                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                
                // Mock existing table with columns
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("existing_table"), eq(null)))
                        .thenReturn(mockExistingTableResultSet);
                when(mockExistingTableResultSet.next()).thenReturn(true, false);
                when(mockExistingTableResultSet.getString("COLUMN_NAME")).thenReturn("id");
                when(mockExistingTableResultSet.getString("TYPE_NAME")).thenReturn("INTEGER");
                when(mockExistingTableResultSet.getInt("DATA_TYPE")).thenReturn(4);
                when(mockExistingTableResultSet.getBoolean("NULLABLE")).thenReturn(false);
                
                // Mock nonexistent table (no columns)
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("nonexistent_table"), eq(null)))
                        .thenReturn(mockNonexistentTableResultSet);
                when(mockNonexistentTableResultSet.next()).thenReturn(false); // No columns

                Map<String, com.firebolt.kafka.connect.TableSchema> result = 
                    fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                assertNotNull(result);
                assertEquals(1, result.size()); // Only the existing table should be included
                assertTrue(result.containsKey("existing_table"));
                assertFalse(result.containsKey("nonexistent_table"));
                
                com.firebolt.kafka.connect.TableSchema schema = result.get("existing_table");
                assertEquals("existing_table", schema.getTableName());
                assertEquals(1, schema.getColumns().size());
            }
        }

        @Test
        void shouldUseCorrectSchemaAndCatalogParameters() throws SQLException {
            Set<String> tableNames = Set.of("test_table");

            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);

                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("test_table"), eq(null)))
                        .thenReturn(mockResultSet);
                when(mockResultSet.next()).thenReturn(false); // No columns for simplicity

                fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                // Verify that the correct parameters are used for getColumns call
                verify(mockMetaData).getColumns(
                    eq(null),       // catalog
                    eq("public"),   // schema
                    eq("test_table"), // table name
                    eq(null)        // column name pattern
                );
            }
        }

        @Test
        void shouldCapturePrecisionAndScaleForNumericAndVarcharColumns() throws SQLException {
            Set<String> tableNames = Set.of("metrics");

            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);

                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("metrics"), eq(null)))
                        .thenReturn(mockResultSet);

                // Two columns: amount NUMERIC(20,5), name VARCHAR(255)
                when(mockResultSet.next()).thenReturn(true, true, false);
                when(mockResultSet.getString("COLUMN_NAME")).thenReturn("amount", "name");
                when(mockResultSet.getString("TYPE_NAME")).thenReturn("NUMERIC", "VARCHAR");
                when(mockResultSet.getInt("DATA_TYPE")).thenReturn(3, 12);
                when(mockResultSet.getBoolean("NULLABLE")).thenReturn(true, true);
                when(mockResultSet.getInt("COLUMN_SIZE")).thenReturn(20, 255);
                when(mockResultSet.getInt("DECIMAL_DIGITS")).thenReturn(5, 0);

                Map<String, com.firebolt.kafka.connect.TableSchema> result =
                        fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                com.firebolt.kafka.connect.TableSchema schema = result.get("metrics");
                assertNotNull(schema);
                assertEquals(2, schema.getColumns().size());

                com.firebolt.kafka.connect.TableSchema.Column amount = schema.getColumns().get(0);
                assertEquals("amount", amount.getName());
                assertEquals(20, amount.getPrecision());
                assertEquals(5, amount.getScale());

                com.firebolt.kafka.connect.TableSchema.Column name = schema.getColumns().get(1);
                assertEquals("name", name.getName());
                assertEquals(255, name.getPrecision());
                assertEquals(0, name.getScale());
            }
        }

        @Test
        void shouldDefaultPrecisionAndScaleToZeroWhenMetadataIsNull() throws SQLException {
            Set<String> tableNames = Set.of("prod");

            Connection mockConnection = mock(Connection.class);
            DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
            ResultSet mockResultSet = mock(ResultSet.class);

            try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
                mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                        .thenReturn(mockConnection);

                when(mockConnection.getMetaData()).thenReturn(mockMetaData);
                PreparedStatement mockPs = mock(PreparedStatement.class);
                ResultSet mockPsRs = mock(ResultSet.class);
                when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
                when(mockPs.executeQuery()).thenReturn(mockPsRs);
                when(mockPsRs.next()).thenReturn(false);
                when(mockMetaData.getColumns(eq(null), eq("public"), eq("prod"), eq(null)))
                        .thenReturn(mockResultSet);

                // One column where COLUMN_SIZE and DECIMAL_DIGITS return SQL NULL -> Mockito getInt returns 0 by default
                when(mockResultSet.next()).thenReturn(true, false);
                when(mockResultSet.getString("COLUMN_NAME")).thenReturn("description");
                when(mockResultSet.getString("TYPE_NAME")).thenReturn("TEXT");
                when(mockResultSet.getInt("DATA_TYPE")).thenReturn(12);
                when(mockResultSet.getBoolean("NULLABLE")).thenReturn(true);
                // Intentionally do not stub COLUMN_SIZE/DECIMAL_DIGITS to simulate NULL -> 0

                Map<String, com.firebolt.kafka.connect.TableSchema> result =
                        fireboltDbService.discoverTableSchemas(jdbcConfig, tableNames);

                com.firebolt.kafka.connect.TableSchema schema = result.get("prod");
                assertNotNull(schema);
                assertEquals(1, schema.getColumns().size());

                com.firebolt.kafka.connect.TableSchema.Column description = schema.getColumns().get(0);
                assertEquals("description", description.getName());
                assertEquals(0, description.getPrecision());
                assertEquals(0, description.getScale());
            }
        }
    }
} 