package com.firebolt.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.service.FireboltDbService;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import org.apache.commons.lang3.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Firebolt Sink Connector for Kafka Connect.
 * This connector streams data from Kafka topics to Firebolt database tables.
 */
@Slf4j
public class FireboltSinkConnector extends SinkConnector {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Firebolt Sink Connector with version {}", version());
        this.configProperties = new HashMap<>(props);
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
            taskConfig.put(FireboltSinkTask.TASK_ID_ATTRIBUTE, String.valueOf(i));
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

    /**
     * Beyond the {@link ConfigDef} validators (syntax), checks that Firebolt is reachable and that every
     * table the connector will write to — the mapped tables and any post-processing tables — exists.
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config result = super.validate(connectorConfigs);
        if (result.configValues().stream().anyMatch(v -> !v.errorMessages().isEmpty())) {
            return result;
        }

        JdbcConfig jdbcConfig = getJdbcConfig(connectorConfigs);
        try {
            fireboltDbService.testConnection(jdbcConfig);
        } catch (ConnectionFailedException e) {
            addError(result, ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "Connection test failed: " + e.getMessage());
            return result;
        } catch (Exception e) {
            addError(result, ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "Unexpected error during connection test: " + e.getMessage());
            return result;
        }

        validateTablesExist(result, jdbcConfig, ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, mappedTableNames(connectorConfigs));
        validateTablesExist(result, jdbcConfig, ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG, postProcessingTableNames(connectorConfigs));
        return result;
    }

    private void validateTablesExist(Config result, JdbcConfig jdbcConfig, String configKey, Set<String> tableNames) {
        if (tableNames.isEmpty()) {
            return;
        }
        try {
            Set<String> missing = fireboltDbService.findNonExistentTables(jdbcConfig, tableNames);
            if (!missing.isEmpty()) {
                addError(result, configKey, "Tables referenced by " + configKey + " do not exist in the database: " + missing);
            }
        } catch (Exception e) {
            addError(result, configKey, "Table existence validation failed: " + e.getMessage());
        }
    }

    private JdbcConfig getJdbcConfig(Map<String, String> connectorConfigs) {
        return new SinkConfig(connectorConfigs).getJdbcConfig();
    }

    /** Tables named in {@code topic.to.table.mapping} (syntax checked by TopicToTableValidator), else the topic names. */
    private Set<String> mappedTableNames(Map<String, String> connectorConfigs) {
        String mapping = connectorConfigs.get(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);
        Stream<String> names = StringUtils.isBlank(mapping)
                ? Arrays.stream(connectorConfigs.getOrDefault(SinkConnector.TOPICS_CONFIG, "").split(","))
                : Arrays.stream(mapping.split(",")).map(entry -> entry.substring(entry.indexOf(':') + 1));
        return names.map(String::trim).filter(name -> !name.isEmpty()).collect(Collectors.toSet());
    }

    /** Tables named in {@code post.processing.script} (its syntax is checked by PostProcessingScriptValidator). */
    private Set<String> postProcessingTableNames(Map<String, String> connectorConfigs) {
        String postProcessing = connectorConfigs.get(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG);
        if (StringUtils.isBlank(postProcessing)) {
            return Set.of();
        }
        try {
            PostProcessingConfig config = OBJECT_MAPPER.readValue(postProcessing.trim(), PostProcessingConfig.class);
            return config.getMappings() == null ? Set.of() : config.getMappings().stream()
                    .map(PostProcessingConfig.Mapping::getTable)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            return Set.of();
        }
    }

    private void addError(Config result, String configKey, String errorMessage) {
        result.configValues().stream()
                .filter(value -> value.name().equals(configKey))
                .findFirst()
                .ifPresent(value -> value.addErrorMessage(errorMessage));
    }
}
