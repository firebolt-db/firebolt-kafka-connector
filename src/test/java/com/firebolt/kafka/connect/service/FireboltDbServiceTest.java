package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
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
    
    /**
     * Helper method to create JdbcConfig for invalid URL tests
     */
    private JdbcConfig createInvalidJdbcConfig(String connectionUrl) {
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
                when(mockConnection.isValid(5)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl)));
                
                verify(mockConnection).isValid(5);
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
                when(mockConnection.isValid(5)).thenReturn(true);

                assertDoesNotThrow(() -> fireboltDbService.testConnection(createJdbcConfig(connectionUrl)));
                
                verify(mockConnection).isValid(5);
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

            assertEquals(5, options.getConnectionTimeoutSeconds(), "Default timeout should be 5 seconds");
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

            assertEquals(5, options.getConnectionTimeoutSeconds(), "Default timeout should be 5 seconds");
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
} 