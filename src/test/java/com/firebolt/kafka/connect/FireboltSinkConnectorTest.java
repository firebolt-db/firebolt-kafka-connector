package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.service.FireboltDbService;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Test class for FireboltSinkConnector.
 */
public class FireboltSinkConnectorTest {
    
    private FireboltSinkConnector connector;
    private Map<String, String> properties;
    
    @Mock
    private FireboltDbService mockFireboltDbService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        connector = new FireboltSinkConnector();
        properties = new HashMap<>();

        // Set up minimal valid configuration
        properties.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:my_database?engine=my_engine&account=my_account");
        properties.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");
    }
    
    @Test
    void testVersion() {
        assertEquals("0.2", connector.version());
    }
    
    @Test
    void testTaskClass() {
        assertEquals(FireboltSinkTask.class, connector.taskClass());
    }
    
    @Test
    void testStart() {
        assertDoesNotThrow(() -> connector.start(properties));
    }
    
    @Test
    void testTaskConfigs() {
        connector.start(properties);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);
        
        assertEquals(3, taskConfigs.size());
        
        // Check that each task config contains the original properties
        for (int i = 0; i < taskConfigs.size(); i++) {
            Map<String, String> taskConfig = taskConfigs.get(i);
            
            // Should contain all original properties
            assertTrue(taskConfig.containsKey(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG));
            assertTrue(taskConfig.containsKey(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG));
            
            // Should contain task-specific properties
            assertEquals(String.valueOf(i), taskConfig.get("task.id"));
        }
    }
    
    @Test
    void testConfig() {
        assertNotNull(connector.config());
        assertEquals(ConnectorConfigDefinition.CONFIG_DEF, connector.config());
    }
    
    @Test
    void testStop() {
        connector.start(properties);
        assertDoesNotThrow(() -> connector.stop());
    }


    @Test
    void testValidateConfig() {
        // Test with valid configuration
        assertDoesNotThrow(() -> connector.validate(properties));
        
        // Test with invalid URL
        Map<String, String> invalidProps = new HashMap<>(properties);
        invalidProps.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "invalid-url");
        
        assertDoesNotThrow(() -> connector.validate(invalidProps));
    }

    @Test
    void testValidateWithValidConfig() throws ConnectionFailedException {
        doNothing().when(mockFireboltDbService).testConnection(any(JdbcConfig.class));
        FireboltSinkConnector connector = new FireboltSinkConnector(mockFireboltDbService);
        Map<String, String> validConfig = new HashMap<>();
        validConfig.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:my_database?engine=my_engine&account=my_account");
        validConfig.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");
        Config result = connector.validate(validConfig);
        for (ConfigValue value : result.configValues()) {
            assertTrue(value.errorMessages().isEmpty(),
                "Expected no error messages for valid config, but got: " + value.errorMessages());
        }
    }

    @Test
    void testValidateWithMissingRequiredFields() {
        FireboltSinkConnector connector = new FireboltSinkConnector();
        Map<String, String> invalidConfig = new HashMap<>();
        // Missing all required fields
        Config result = connector.validate(invalidConfig);
        boolean hasErrors = result.configValues().stream().anyMatch(v -> !v.errorMessages().isEmpty());
        assertTrue(hasErrors, "Expected errors for missing required fields");
    }

    @Nested
    class ConnectionValidationTests {

        @Test
        void shouldPassValidationWhenConnectionTestSucceeds() throws ConnectionFailedException {
            FireboltSinkConnector connectorWithMock = new FireboltSinkConnector(mockFireboltDbService);
            doNothing().when(mockFireboltDbService).testConnection(any(JdbcConfig.class));
            
            Map<String, String> validConfig = new HashMap<>();
            validConfig.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:my_database?engine=my_engine&account=my_account");
            validConfig.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");

            Config result = connectorWithMock.validate(validConfig);
            
            ConfigValue urlValue = result.configValues().stream()
                .filter(v -> v.name().equals(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG))
                .findFirst().orElse(null);
            
            assertNotNull(urlValue);
            assertTrue(urlValue.errorMessages().isEmpty(), "Expected no error messages when connection succeeds");
            verify(mockFireboltDbService).testConnection(any(JdbcConfig.class));
        }

        @Test
        void shouldAddErrorWhenConnectionTestFails() throws ConnectionFailedException {
            FireboltSinkConnector connectorWithMock = new FireboltSinkConnector(mockFireboltDbService);
            String errorMessage = "Failed to connect to database";
            doThrow(new ConnectionFailedException(errorMessage)).when(mockFireboltDbService).testConnection(any(JdbcConfig.class));
            
            Map<String, String> configWithBadUrl = new HashMap<>();
            configWithBadUrl.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:bad_database?engine=the_engine&account=the_account");
            configWithBadUrl.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");

            Config result = connectorWithMock.validate(configWithBadUrl);
            
            ConfigValue urlValue = result.configValues().stream()
                .filter(v -> v.name().equals(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG))
                .findFirst().orElse(null);
            
            assertNotNull(urlValue);
            assertFalse(urlValue.errorMessages().isEmpty(), "Expected error messages when connection fails");
            assertTrue(urlValue.errorMessages().get(0).contains("Connection test failed"));
            assertTrue(urlValue.errorMessages().get(0).contains(errorMessage));
            verify(mockFireboltDbService).testConnection(any(JdbcConfig.class));
        }

        @Test
        void shouldAddErrorWhenUnexpectedExceptionOccurs() throws ConnectionFailedException {
            FireboltSinkConnector connectorWithMock = new FireboltSinkConnector(mockFireboltDbService);
            String errorMessage = "Unexpected network error";
            doThrow(new RuntimeException(errorMessage)).when(mockFireboltDbService).testConnection(any(JdbcConfig.class));
            
            Map<String, String> config = new HashMap<>();
            config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:test_database?engine=the_engine&account=the_account");
            config.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");

            Config result = connectorWithMock.validate(config);
            
            ConfigValue urlValue = result.configValues().stream()
                .filter(v -> v.name().equals(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG))
                .findFirst().orElse(null);
            
            assertNotNull(urlValue);
            assertFalse(urlValue.errorMessages().isEmpty(), "Expected error messages when unexpected exception occurs");
            assertTrue(urlValue.errorMessages().get(0).contains("Unexpected error during connection test"));
            assertTrue(urlValue.errorMessages().get(0).contains(errorMessage));
            verify(mockFireboltDbService).testConnection(any(JdbcConfig.class));
        }

        @Test
        void shouldNotCallValidateConnectionConfigWhenBasicValidationFails() throws ConnectionFailedException {
            FireboltSinkConnector connectorWithMock = new FireboltSinkConnector(mockFireboltDbService);
            
            // Config with missing required JDBC URL - will fail basic validation
            Map<String, String> configWithNullUrl = new HashMap<>();
            configWithNullUrl.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");

            connectorWithMock.validate(configWithNullUrl);
            
            verify(mockFireboltDbService, never()).testConnection(any(JdbcConfig.class));
        }
    }
}