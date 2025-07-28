package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple configuration wrapper for the Firebolt Sink Connector.
 * This class provides getter methods for configuration values using ConnectorConfigDefinition constants.
 */
@Slf4j
public class SinkConfig {

    private final Map<String, String> config;

    public SinkConfig(Map<String, String> config) {
        this.config = config;
    }

    public String getTopicToTableMapping() {
        return config.get(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);
    }

    public String getTableNameForTopic(String topic) {
        String mapping = getTopicToTableMapping();
        if (mapping == null || mapping.trim().isEmpty()) {
            return null;
        }

        String[] mappings = mapping.split(",");
        for (String map : mappings) {
            String trimmed = map.trim();
            String[] parts = trimmed.split(":");
            if (parts.length == 2 && parts[0].trim().equals(topic)) {
                return parts[1].trim();
            }
        }

        return null;
    }

    public JdbcConfig getJdbcConfig() {
        return JdbcConfig.builder()
                .jdbcConnectionUrl(config.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG))
                .clientId(Optional.ofNullable(config.get(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG)))
                .clientSecret(Optional.ofNullable(config.get(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG)))
                .build();
    }

    // Utility method to get any config value
    public String get(String key) {
        return config.get(key);
    }

    // Utility method to get config map
    public Map<String, String> getConfig() {
        return config;
    }
}