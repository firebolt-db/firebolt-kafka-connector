package com.firebolt.kafka.connect.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for ConnectorConfigDefinition.
 */
class ConnectorConfigDefinitionTest {

    @Nested
    class ConfigurationConstantsTests {

        @Test
        void shouldHaveCorrectJdbcConnectionUrlConfig() {
            assertEquals("jdbc.connection.url", ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG);
            assertNotNull(ConnectorConfigDefinition.JDBC_CONNECTION_URL_DOC);
            assertTrue(ConnectorConfigDefinition.JDBC_CONNECTION_URL_DOC.contains("Firebolt JDBC"));
            assertNull(ConnectorConfigDefinition.JDBC_CONNECTION_URL_DEFAULT);
        }

        @Test
        void shouldHaveCorrectTopicToTableMappingConfig() {
            assertEquals("topic.to.table.mapping", ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);
            assertNotNull(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_DOC);
            assertTrue(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_DOC.contains("Comma-separated"));
            assertNull(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_DEFAULT);
        }

        @Test
        void shouldHaveCorrectFireboltClientIdConfig() {
            assertEquals("firebolt.clientId", ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG);
            assertNotNull(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_DOC);
            assertTrue(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_DOC.contains("client id"));
            assertTrue(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_DOC.contains("Firebolt account"));
            assertNull(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_DEFAULT);
        }

        @Test
        void shouldHaveCorrectFireboltClientSecretConfig() {
            assertEquals("firebolt.clientSecret", ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG);
            assertNotNull(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_DOC);
            assertTrue(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_DOC.contains("client secret"));
            assertTrue(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_DOC.contains("Firebolt account"));
            assertNull(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_DEFAULT);
        }
    }

    @Nested
    class ConfigDefCreationTests {

        @Test
        void shouldCreateValidConfigDef() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            assertNotNull(configDef);
            
            // Verify all expected configuration keys are present
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.ERROR_TOLERANCE_CONFIG));
            assertTrue(configDef.names().contains(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG));
        }

        @Test
        void shouldHaveCorrectNumberOfConfigProperties() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            assertEquals(9, configDef.names().size());
        }

        @Test
        void shouldHaveCorrectConfigurationTypes() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            
            assertEquals(ConfigDef.Type.STRING, 
                configDef.configKeys().get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG).type);
            assertEquals(ConfigDef.Type.PASSWORD, 
                configDef.configKeys().get(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG).type);
            assertEquals(ConfigDef.Type.PASSWORD, 
                configDef.configKeys().get(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG).type);
            assertEquals(ConfigDef.Type.BOOLEAN,
                configDef.configKeys().get(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG).type);
            assertEquals(ConfigDef.Type.STRING,
                configDef.configKeys().get(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG).type);
            assertEquals(ConfigDef.Type.STRING, 
                configDef.configKeys().get(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG).type);
            assertEquals(ConfigDef.Type.STRING,
                configDef.configKeys().get(ConnectorConfigDefinition.ERROR_TOLERANCE_CONFIG).type);
            assertEquals(ConfigDef.Type.STRING,
                configDef.configKeys().get(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG).type);
        }

        @Test
        void shouldHaveCorrectImportanceLevels() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            
            assertEquals(ConfigDef.Importance.HIGH, 
                configDef.configKeys().get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG).importance);
            assertEquals(ConfigDef.Importance.HIGH, 
                configDef.configKeys().get(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG).importance);
            assertEquals(ConfigDef.Importance.HIGH, 
                configDef.configKeys().get(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG).importance);
            assertEquals(ConfigDef.Importance.HIGH,
                configDef.configKeys().get(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG).importance);
            assertEquals(ConfigDef.Importance.HIGH,
                configDef.configKeys().get(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG).importance);
            assertEquals(ConfigDef.Importance.HIGH, 
                configDef.configKeys().get(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG).importance);
            assertEquals(ConfigDef.Importance.MEDIUM,
                configDef.configKeys().get(ConnectorConfigDefinition.ERROR_TOLERANCE_CONFIG).importance);
            assertEquals(ConfigDef.Importance.MEDIUM,
                configDef.configKeys().get(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG).importance);
        }
    }

    @Nested
    class ConfigurationValidationTests {

        @Test
        void shouldValidateValidConfiguration() {
            Map<String, String> validConfig = createValidConfig();

            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(validConfig));
        }

        @Test
        void shouldRejectInvalidJdbcConnectionUrl() {
            Map<String, String> config = createValidConfig();
            config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "invalid-url");
            
            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasErrors = result.stream().anyMatch(v -> !v.errorMessages().isEmpty());
            assertTrue(hasErrors, "Expected validation errors for invalid JDBC connection URL");
        }

        @Test
        void shouldAcceptValidJdbcConnectionUrl() {
            Map<String, String> config = createValidConfig();
            config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:my_database?engine=my_engine&account=my_account");
            
            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(config));
        }

        @Test
        void shouldRejectEmptyJdbcConnectionUrl() {
            Map<String, String> config = createValidConfig();
            config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "");
            
            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasErrors = result.stream().anyMatch(v -> !v.errorMessages().isEmpty());
            assertTrue(hasErrors, "Expected validation errors for empty JDBC connection URL");
        }

        @Test
        void shouldRejectWhitespaceOnlyJdbcConnectionUrl() {
            Map<String, String> config = createValidConfig();
            config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "   ");
            
            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasErrors = result.stream().anyMatch(v -> !v.errorMessages().isEmpty());
            assertTrue(hasErrors, "Expected validation errors for whitespace-only JDBC connection URL");
        }

        @Test
        void shouldRejectInvalidPostProcessingScriptJson() {
            Map<String, String> config = createValidConfig();
            config.put(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG, "{ invalid json");

            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasErrors = result.stream()
                    .filter(v -> ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG.equals(v.name()))
                    .anyMatch(v -> !v.errorMessages().isEmpty());
            assertTrue(hasErrors, "Expected validation errors for invalid post.processing.script JSON");
        }

        @Test
        void shouldAcceptValidPostProcessingScriptJson() {
            Map<String, String> config = createValidConfig();
            String json = "{\"mappings\":[{\"table\":\"table1\",\"script\":\"UPDATE \\\"table1\\\" SET processed = true\"}]}";
            config.put(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG, json);

            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasErrors = result.stream()
                    .filter(v -> ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG.equals(v.name()))
                    .anyMatch(v -> !v.errorMessages().isEmpty());
            assertFalse(hasErrors, "Did not expect validation errors for valid post.processing.script JSON");
        }

        @Test
        void shouldAcceptExactlyOnceTrue() {
            Map<String, String> config = new HashMap<>(createValidConfig());
            config.put(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG, "true");

            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(config));
        }

        @Test
        void shouldAcceptExactlyOnceFalse() {
            Map<String, String> config = new HashMap<>(createValidConfig());
            config.put(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG, "false");

            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(config));
        }

        @Test
        void shouldRejectUnknownExactlyOnceValue() {
            Map<String, String> config = new HashMap<>(createValidConfig());
            config.put(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG, "unknown");

            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasExactlyOnceErrors = result.stream()
                .filter(v -> ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG.equals(v.name()))
                .anyMatch(v -> !v.errorMessages().isEmpty());
            assertTrue(hasExactlyOnceErrors, "Expected validation errors for invalid exactlyOnce value");
        }

        @Test
        void shouldAcceptValidIngestionTypeValues() {
            Map<String, String> configSql = createValidConfig();
            configSql.put(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG, "sql");
            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(configSql));

            Map<String, String> configBinary = createValidConfig();
            configBinary.put(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG, "binary");
            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(configBinary));
        }

        @Test
        void shouldRejectInvalidIngestionType() {
            Map<String, String> config = createValidConfig();
            config.put(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG, "invalid");
            var result = ConnectorConfigDefinition.CONFIG_DEF.validate(config);
            boolean hasErrors = result.stream()
                .filter(v -> ConnectorConfigDefinition.INGESTION_TYPE_CONFIG.equals(v.name()))
                .anyMatch(v -> !v.errorMessages().isEmpty());
            assertTrue(hasErrors, "Expected validation errors for invalid ingestion.type value");
        }
    }

    @Nested
    class DefaultValuesTests {

        @Test
        void shouldUseDefaultValuesWhenNotProvided() {
            Map<String, String> config = new HashMap<>();
            config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:my_database?engine=my_engine&account=my_account");
            config.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");
            
            assertDoesNotThrow(() -> ConnectorConfigDefinition.CONFIG_DEF.validate(config));
        }

        @Test
        void shouldDefaultExactlyOnceToFalseWhenNotProvided() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            Object defaultValue = configDef.configKeys().get(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG).defaultValue;
            assertEquals(Boolean.FALSE, defaultValue);
        }

        @Test
        void shouldDefaultIngestionTypeToSqlWhenNotProvided() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            Object defaultValue = configDef.configKeys().get(ConnectorConfigDefinition.INGESTION_TYPE_CONFIG).defaultValue;
            assertEquals("sql", defaultValue);
        }
    }

    @Nested
    class ConfigurationDocumentationTests {

        @Test
        void shouldHaveDocumentationForAllConfigurationProperties() {
            ConfigDef configDef = ConnectorConfigDefinition.CONFIG_DEF;
            
            for (String configName : configDef.names()) {
                ConfigDef.ConfigKey configKey = configDef.configKeys().get(configName);
                assertNotNull(configKey.documentation, "Documentation missing for: " + configName);
                assertFalse(configKey.documentation.trim().isEmpty(), "Empty documentation for: " + configName);
            }
        }

        @Test
        void shouldHaveMeaningfulDocumentationContent() {
            assertTrue(ConnectorConfigDefinition.JDBC_CONNECTION_URL_DOC.contains("Firebolt JDBC"));
            assertTrue(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_DOC.contains("client id"));
            assertTrue(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_DOC.contains("client secret"));
            assertTrue(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_DOC.contains("Comma-separated"));
            assertTrue(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_DOC.toLowerCase().contains("exactly-once"));
            assertTrue(ConnectorConfigDefinition.ERROR_TOLERANCE_DOC.contains("Error tolerance policy"));
            assertTrue(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_DOC.toLowerCase().contains("post-processing"));
        }
    }

    /**
     * Creates a valid configuration map for testing.
     */
    private Map<String, String> createValidConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:my_database?engine=my_engine&account=my_account");
        config.put(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG, "test_client_id");
        config.put(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG, "test_client_secret");
        config.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1");
        config.put(ConnectorConfigDefinition.ERROR_TOLERANCE_CONFIG, "all");
        return config;
    }
} 