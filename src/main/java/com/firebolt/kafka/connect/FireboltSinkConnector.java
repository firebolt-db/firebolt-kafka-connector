package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.config.TopicToTableValidator;
import com.firebolt.kafka.connect.service.FireboltDbService;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import com.firebolt.shadow.org.apache.commons.lang3.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Firebolt Sink Connector for Kafka Connect.
 * This connector streams data from Kafka topics to Firebolt database tables.
 */
@Slf4j
public class FireboltSinkConnector extends SinkConnector {
    
    private Map<String, String> configProperties;
    private FireboltDbService fireboltDbService;

    public FireboltSinkConnector() {
        this(new FireboltDbService());
    }

    @VisibleForTesting
    FireboltSinkConnector(FireboltDbService fireboltDbService) {
        this.fireboltDbService = fireboltDbService;
    }

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
        this.configProperties = new HashMap<>(props);
        
        // Validate the configuration using the config definition
        ConnectorConfigDefinition.CONFIG_DEF.validate(props);
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
            Map<String, String> taskConfig = new HashMap<>(configProperties);
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

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        log.info("Validating connector configuration");
        
        // Get the default validation from parent
        Config result = super.validate(connectorConfigs);

        // if there are already validation issues here
        for (ConfigValue v : result.configValues()) {
            if (!v.errorMessages().isEmpty()) {
                return result;
            }
        }

        // Add custom validation logic
        List<ConfigValue> configValues = new ArrayList<>(result.configValues());
        
        // Only validate connection if JDBC URL is present
        String jdbcUrl = connectorConfigs.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG);
        if (!StringUtils.isEmpty(jdbcUrl)) {
            // Validate JDBC connection url and parameters by making sure we can connect to that url
            validateConnectionConfig(connectorConfigs, configValues);
            
            // Validate table configuration
            validateTableConfig(connectorConfigs, configValues);
        }
        
        return new Config(configValues);
    }
    
    /**
     * JdbcConnectionUrlValidator handles just the syntactic validation. Need to check if the connection can be established.
     * @param connectorConfigs
     * @param configValues
     */
    private void validateConnectionConfig(Map<String, String> connectorConfigs, 
                                        List<ConfigValue> configValues) {
        try {
            JdbcConfig jdbcConfig = getJdbcConfig(connectorConfigs);
            fireboltDbService.testConnection(jdbcConfig);
            log.info("Connection validation successful for JDBC URL");
        } catch (ConnectionFailedException e) {
            log.warn("Connection validation failed: {}", e.getMessage());
            addErrorToConfig(configValues, 
                           ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG,
                           "Connection test failed: " + e.getMessage());
        } catch (Exception e) {
            log.warn("Unexpected error during connection validation: {}", e.getMessage());
            addErrorToConfig(configValues, 
                           ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG,
                           "Unexpected error during connection test: " + e.getMessage());
        }
    }

    /**
     * Additional table configuration validation.
     * Basic validation is already handled by {@link TopicToTableValidator} in the ConfigDef.
     * This method validates that the specified tables actually exist in the database.
     *
     * NOTE: if the topic.to.table.mapping is empty, then we will check the topics name as table names to exist in the database
     * 
     * @param connectorConfigs the connector configuration
     * @param configValues the list of config values to update with any errors
     */
    private void validateTableConfig(Map<String, String> connectorConfigs, 
                                   List<ConfigValue> configValues) {
        String jdbcUrl = connectorConfigs.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG);

        // Only validate table existence if we have a valid connection URL
        if (StringUtils.isEmpty(jdbcUrl)) {
            log.warn("Skipping table existence validation: missing JDBC URL or table mapping");
            return;
        }

        // Check if connection validation already failed - if so, skip table validation
        ConfigValue connectionConfigValue = findConfigValue(configValues, ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG);
        if (connectionConfigValue != null && !connectionConfigValue.errorMessages().isEmpty()) {
            log.debug("Skipping table existence validation: connection validation failed");
            return;
        }
        
        try {
            // get the table names that we need to validate
            Set<String> tableNames = getTableNames(connectorConfigs);
            
            if (!tableNames.isEmpty()) {
                Set<String> nonExistentTables = fireboltDbService.validateTablesExist(getJdbcConfig(connectorConfigs), tableNames);
                
                if (!nonExistentTables.isEmpty()) {
                    String errorMessage = String.format("The following tables do not exist in the database: %s. " +
                        "All the tables need to be exist in the database if used in topic.to.table.mapping",
                        nonExistentTables);
                    addErrorToConfig(configValues, 
                                   ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG,
                                   errorMessage);
                    log.warn("Table existence validation failed: {}", errorMessage);
                }
            }
            log.info("Table existence validation completed successfully");
        } catch (ConnectionFailedException e) {
            log.warn("Error during table existence validation: {}", e.getMessage());
            addErrorToConfig(configValues, 
                           ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG,
                           "Table existence validation failed: " + e.getMessage());
        } catch (Exception e) {
            log.warn("Unexpected error during table existence validation: {}", e.getMessage());
            addErrorToConfig(configValues, 
                           ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG,
                           "Table existence validation failed: " + e.getMessage());
        }
    }

    private JdbcConfig getJdbcConfig(Map<String, String> connectorConfigs) {
        return JdbcConfig.builder()
                .jdbcConnectionUrl(connectorConfigs.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG))
                .clientId(Optional.ofNullable(connectorConfigs.get(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG)))
                .clientSecret(Optional.ofNullable(connectorConfigs.get(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG)))
                .build();
    }

    private Set<String> getTableNames(Map<String, String> connectorConfigs) {
        String topicToTableMapping = connectorConfigs.get(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);

        if (!StringUtils.isEmpty(topicToTableMapping)) {
            log.debug("Using the topic.to.table.mapping for determining the firebolt tables");
            return parseTableNamesFromMapping(topicToTableMapping);
        }

        // use topics if there is no mapping
        log.debug("Using the topics values as target tables in Firebolt");
        String topics = connectorConfigs.get(SinkConnector.TOPICS_CONFIG);
        return Arrays.stream(topics.split(",")).collect(Collectors.toSet());
    }

    
    /**
     * Parses table names from the topic-to-table mapping string.
     * Expected format: "topic1:table1,topic2:table2"
     * 
     * @param topicToTableMapping the mapping string
     * @return set of unique table names
     */
    private Set<String> parseTableNamesFromMapping(String topicToTableMapping) {
        Set<String> tableNames = new HashSet<>();
        
        if (topicToTableMapping == null || topicToTableMapping.trim().isEmpty()) {
            return tableNames;
        }
        
        String[] mappings = topicToTableMapping.split(",");
        for (String mapping : mappings) {
            String trimmedMapping = mapping.trim();
            if (trimmedMapping.contains(":")) {
                String[] parts = trimmedMapping.split(":");
                if (parts.length == 2) {
                    String tableName = parts[1].trim();
                    if (!tableName.isEmpty()) {
                        tableNames.add(tableName);
                    }
                }
            }
        }
        
        log.debug("Parsed table names from mapping '{}': {}", topicToTableMapping, tableNames);
        return tableNames;
    }

    
    private void addErrorToConfig(List<ConfigValue> configValues, String configKey, String errorMessage) {
        ConfigValue configValue = findConfigValue(configValues, configKey);
        if (configValue != null) {
            configValue.addErrorMessage(errorMessage);
        }
    }
    
    private ConfigValue findConfigValue(List<ConfigValue> configValues, String name) {
        return configValues.stream()
                .filter(cv -> cv.name().equals(name))
                .findFirst()
                .orElse(null);
    }

}