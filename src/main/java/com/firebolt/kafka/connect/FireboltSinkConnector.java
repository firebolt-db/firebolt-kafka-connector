package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Firebolt Sink Connector for Kafka Connect.
 * This connector streams data from Kafka topics to Firebolt database tables.
 */
@Slf4j
public class FireboltSinkConnector extends SinkConnector {
    
    @Override
    public String version() {
        try {
            Properties properties = new Properties();
            try (InputStream input = getClass().getClassLoader().getResourceAsStream("version.properties")) {
                if (input != null) {
                    properties.load(input);
                    return properties.getProperty("version", "unknown");
                }
            }
        } catch (IOException e) {
            log.warn("Failed to load version from properties file", e);
        }
        return "unknown";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Firebolt Sink Connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FireboltSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configurations", maxTasks);
        
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>();
            // Add task-specific configuration if needed
            taskConfig.put("task.id", String.valueOf(i));
            configs.add(taskConfig);
        }
        
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping Firebolt Sink Connector");
    }

    @Override
    public ConfigDef config() {
        return ConnectorConfigDefinition.CONFIG_DEF;
    }

} 