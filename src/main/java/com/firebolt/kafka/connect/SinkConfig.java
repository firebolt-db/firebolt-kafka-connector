package com.firebolt.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Simple configuration wrapper for the Firebolt Sink Connector.
 * This class provides getter methods for configuration values using ConnectorConfigDefinition constants.
 */
@Slf4j
public class SinkConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
        String jdbcUrl = config.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG);
        if (StringUtils.isNotBlank(jdbcUrl)) {
            jdbcUrl = jdbcUrl + (isOptimizeInserts() ? "&merge_prepared_statement_batches_v2=true" : "");
        }
        return JdbcConfig.builder()
                .jdbcConnectionUrl(jdbcUrl)
                .clientId(Optional.ofNullable(config.get(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG)))
                .clientSecret(Optional.ofNullable(config.get(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG)))
                .build();
    }

    public Optional<String> getPostProcessingScript(String tableName) {
        String postProcessingConfigAsString = config.get(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG);
        if (StringUtils.isBlank(postProcessingConfigAsString)) {
            return Optional.empty();
        }

        try {
            PostProcessingConfig postProcessingConfig = OBJECT_MAPPER.readValue(postProcessingConfigAsString, PostProcessingConfig.class);
            List<PostProcessingConfig.Mapping> mappings = postProcessingConfig.getMappings();
            if (!CollectionUtils.isEmpty(mappings)) {
                return mappings.stream()
                        .filter(mapping -> tableName.equals(mapping.getTable()))
                        .map(PostProcessingConfig.Mapping::getScript)
                        .findFirst();
            }
        } catch (JsonProcessingException e) {
            log.error("Post processing config is not valid", e.getMessage());
        }

        return Optional.empty();
    }

    // Utility method to get any config value
    public String get(String key) {
        return config.get(key);
    }

    // Utility method to get config map
    public Map<String, String> getConfig() {
        return config;
    }

    public boolean isErrorToleranceAll() {
        String tol = config.get(ConnectorConfigDefinition.ERROR_TOLERANCE_CONFIG);
        return tol != null && tol.equalsIgnoreCase("all");
    }

    /**
     * Returns true if the connector should push messages to firebolt exactly once. When false it will do at-least once semantics.
     */
    public boolean isExactlyOnce() {
        return Boolean.parseBoolean(config.get(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG));
    }

    /**
     * Returns true if insert operations should be optimized for better performance.
     */
    public boolean isOptimizeInserts() {
        String value = config.get(ConnectorConfigDefinition.OPTIMIZE_INSERTS_CONFIG);
        return value != null && Boolean.parseBoolean(value);
    }
}