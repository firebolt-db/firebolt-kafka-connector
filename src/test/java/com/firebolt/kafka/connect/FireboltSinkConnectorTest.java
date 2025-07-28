package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FireboltSinkConnectorTest {
    
    private FireboltSinkConnector connector;
    private Map<String, String> properties;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        connector = new FireboltSinkConnector();
        properties = new HashMap<>();
    }
    
    @Test
    void testVersion() {
        assertEquals("0.1", connector.version());
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
    void testConfig() {
        assertNotNull(connector.config());
        assertEquals(ConnectorConfigDefinition.CONFIG_DEF, connector.config());
    }
    
    @Test
    @DisplayName("Should stop without throwing exception")
    void testStop() {
        connector.start(properties);
        assertDoesNotThrow(() -> connector.stop());
    }

} 