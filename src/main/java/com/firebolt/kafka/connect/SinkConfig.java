package com.firebolt.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        return JdbcConfig.builder()
                .jdbcConnectionUrl(config.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG))
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
                        .map(this::getScriptContent)
                        .findFirst();
            }
        } catch (JsonProcessingException e) {
            log.error("Post processing config is not valid", e.getMessage());
        }

        return Optional.empty();
    }

    /**
     * Gets the script content from either the inline script or script file.
     *
     * @param mapping the mapping containing either script or scriptFile
     * @return the script content
     */
    private String getScriptContent(PostProcessingConfig.Mapping mapping) {
        if (StringUtils.isNotBlank(mapping.getScript())) {
            log.debug("Using inline script for table: {}", mapping.getTable());
            return mapping.getScript();
        }

        if (StringUtils.isNotBlank(mapping.getScriptFile())) {
            try {
                Path scriptPath = Paths.get(mapping.getScriptFile());
                log.info("Attempting to read script file: {} (absolute path: {})", mapping.getScriptFile(), scriptPath.toAbsolutePath());
                if (!Files.exists(scriptPath)) {
                    log.error("Script file does not exist: {} (absolute path: {})", mapping.getScriptFile(), scriptPath.toAbsolutePath());
                    throw new RuntimeException("Script file does not exist: " + mapping.getScriptFile());
                }
                String scriptContent = Files.readString(scriptPath);
                log.info("Successfully read script file: {} (content length: {} chars)", mapping.getScriptFile(), scriptContent.length());
                return scriptContent;
            } catch (IOException e) {
                log.error("Failed to read script file: {} (absolute path: {})", mapping.getScriptFile(), Paths.get(mapping.getScriptFile()).toAbsolutePath(), e);
                throw new RuntimeException("Failed to read script file: " + mapping.getScriptFile(), e);
            }
        }

        throw new IllegalStateException("Neither script nor scriptFile is specified for table: " + mapping.getTable());
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

    public boolean isSchemaRefreshEnabled() {
        String val = config.get(ConnectorConfigDefinition.SCHEMA_REFRESH_ENABLED_CONFIG);
        if (val == null) {
            return ConnectorConfigDefinition.SCHEMA_REFRESH_ENABLED_DEFAULT;
        }
        return Boolean.parseBoolean(val);
    }

    public long getSchemaRefreshIntervalMs() {
        String val = config.get(ConnectorConfigDefinition.SCHEMA_REFRESH_INTERVAL_MS_CONFIG);
        if (val == null) {
            return ConnectorConfigDefinition.SCHEMA_REFRESH_INTERVAL_MS_DEFAULT;
        }
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return ConnectorConfigDefinition.SCHEMA_REFRESH_INTERVAL_MS_DEFAULT;
        }
    }

    public IngestionType getIngestionType() {
        String value = config.get(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG);
        if (StringUtils.isBlank(value)) {
            // by default ingestion type should be sql if not passed in
            return IngestionType.SQL;
        } else {
            return IngestionType.fromValue(value);
        }


    }
}