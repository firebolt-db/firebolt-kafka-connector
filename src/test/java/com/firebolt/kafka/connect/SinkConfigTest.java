package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SinkConfigTest {

    private Map<String, String> configMap;
    private SinkConfig sinkConfig;

    @BeforeEach
    void setUp() {
        configMap = new HashMap<>();
        configMap.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, "jdbc:firebolt:test_db");
        configMap.put(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG, "test_client_id");
        configMap.put(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG, "test_client_secret");
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,topic2:table2");
        
        sinkConfig = new SinkConfig(configMap);
    }

    @Test
    void testConstructorWithConfigMap() {
        Map<String, String> testConfig = new HashMap<>();
        testConfig.put("test.key", "test.value");
        
        SinkConfig testSinkConfig = new SinkConfig(testConfig);
        
        assertNotNull(testSinkConfig);
        assertEquals("test.value", testSinkConfig.get("test.key"));
    }

    @Test
    void testConstructorWithEmptyConfig() {
        Map<String, String> emptyConfig = new HashMap<>();
        
        SinkConfig testSinkConfig = new SinkConfig(emptyConfig);
        
        assertNotNull(testSinkConfig);
        assertNull(testSinkConfig.get("any.key"));
    }

    @Test
    void testGetTopicToTableMapping() {
        String result = sinkConfig.getTopicToTableMapping();
        
        assertEquals("topic1:table1,topic2:table2", result);
    }

    @Test
    void testGetTopicToTableMappingWhenNull() {
        configMap.remove(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTopicToTableMapping();
        
        assertNull(result);
    }

    // =========================
    // TOPIC-TO-TABLE MAPPING TESTS
    // =========================

    @Test
    void shouldHandleAllValidTopicToTableMappingScenarios() {
        // --- Explicit match: setUp config has "topic1:table1,topic2:table2" ---
        assertEquals("table1", sinkConfig.getTableNameForTopic("topic1"));
        assertEquals("table2", sinkConfig.getTableNameForTopic("topic2"));

        // --- Single mapping ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "orders:orders_table");
        assertEquals("orders_table", new SinkConfig(configMap).getTableNameForTopic("orders"));

        // --- Multiple mappings with three entries ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG,
                "user-events:user_events_table,order-data:orders_table,metrics:metrics_table");
        SinkConfig complex = new SinkConfig(configMap);
        assertEquals("user_events_table", complex.getTableNameForTopic("user-events"));
        assertEquals("orders_table",      complex.getTableNameForTopic("order-data"));
        assertEquals("metrics_table",     complex.getTableNameForTopic("metrics"));

        // --- Whitespace around separators is trimmed ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, " topic1 : table1 , topic2 : table2 ");
        SinkConfig spaced = new SinkConfig(configMap);
        assertEquals("table1", spaced.getTableNameForTopic("topic1"));
        assertEquals("table2", spaced.getTableNameForTopic("topic2"));

        // --- Various naming conventions: dashes, underscores, dots ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic-with-dashes:table_with_underscores");
        assertEquals("table_with_underscores", new SinkConfig(configMap).getTableNameForTopic("topic-with-dashes"));
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "test.topic:test.table");
        assertEquals("test.table", new SinkConfig(configMap).getTableNameForTopic("test.topic"));
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "metrics_topic:metrics_db_table");
        assertEquals("metrics_db_table", new SinkConfig(configMap).getTableNameForTopic("metrics_topic"));

        // --- Case-sensitive: exact match wins; wrong-case falls back to topic name ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "Topic1:Table1,topic2:table2");
        SinkConfig caseSensitive = new SinkConfig(configMap);
        assertEquals("Table1", caseSensitive.getTableNameForTopic("Topic1"));
        assertEquals("table2", caseSensitive.getTableNameForTopic("topic2"));
        assertEquals("topic1", caseSensitive.getTableNameForTopic("topic1")); // wrong case → fallback
        assertEquals("TOPIC2", caseSensitive.getTableNameForTopic("TOPIC2")); // wrong case → fallback

        // --- Duplicate topic entries: first match wins ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,topic1:table2");
        assertEquals("table1", new SinkConfig(configMap).getTableNameForTopic("topic1"));

        // --- Unmapped topic: fallback to topic name when a mapping is configured but topic is absent ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,topic2:table2");
        SinkConfig withMapping = new SinkConfig(configMap);
        assertEquals("topic3",         withMapping.getTableNameForTopic("topic3"));
        assertEquals("TOPIC1",         withMapping.getTableNameForTopic("TOPIC1"));
        assertEquals("nonexistent",    withMapping.getTableNameForTopic("nonexistent"));

        // --- No mapping configured at all: every topic falls back to its own name ---
        configMap.remove(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);
        SinkConfig noMapping = new SinkConfig(configMap);
        assertEquals("orders",      noMapping.getTableNameForTopic("orders"));
        assertEquals("user-events", noMapping.getTableNameForTopic("user-events"));
        assertEquals("metrics",     noMapping.getTableNameForTopic("metrics"));

        // --- Empty or whitespace-only mapping string: treated as unconfigured, fallback applies ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "");
        assertEquals("any-topic", new SinkConfig(configMap).getTableNameForTopic("any-topic"));
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "   ");
        assertEquals("any-topic", new SinkConfig(configMap).getTableNameForTopic("any-topic"));

        // --- Null/empty topic name: falls back to the topic itself (null → null, "" → "") ---
        assertEquals("", sinkConfig.getTableNameForTopic(""));
        assertNull(sinkConfig.getTableNameForTopic(null));
    }

    @Test
    void shouldRejectAllMalformedTopicToTableMappingConfigurations() {
        // A configured-but-malformed mapping is always a misconfiguration and must throw
        // ConfigException. The fallback (topic → topic) applies only when NO mapping is configured.

        // --- No colon separator ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1");
        assertThrows(ConfigException.class, () -> new SinkConfig(configMap).getTableNameForTopic("topic1"),
                "Entry without a colon separator must throw");

        // --- Too many colons ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1:extra");
        assertThrows(ConfigException.class, () -> new SinkConfig(configMap).getTableNameForTopic("topic1"),
                "Entry with extra colons must throw");

        // --- Empty table name ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:");
        assertThrows(ConfigException.class, () -> new SinkConfig(configMap).getTableNameForTopic("topic1"),
                "Empty table name must throw");

        // --- Empty topic name ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, ":table1");
        assertThrows(ConfigException.class, () -> new SinkConfig(configMap).getTableNameForTopic("topic1"),
                "Empty topic name in mapping must throw");

        // --- Malformed trailing entry: valid first entry returned before bad entry; iterating past it throws ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,malformed");
        SinkConfig trailingMalformed = new SinkConfig(configMap);
        assertEquals("table1", trailingMalformed.getTableNameForTopic("topic1")); // found before bad entry
        assertThrows(ConfigException.class, () -> trailingMalformed.getTableNameForTopic("other-topic"),
                "Malformed trailing entry must throw when scanned");

        // --- Trailing colon-only entry (empty topic and table) ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,:");
        SinkConfig trailingColonOnly = new SinkConfig(configMap);
        assertEquals("table1", trailingColonOnly.getTableNameForTopic("topic1"));
        assertThrows(ConfigException.class, () -> trailingColonOnly.getTableNameForTopic("other-topic"),
                "Trailing colon-only entry must throw when scanned");

        // --- Trailing comma (produces an empty string entry) ---
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,");
        SinkConfig trailingComma = new SinkConfig(configMap);
        assertEquals("table1", trailingComma.getTableNameForTopic("topic1"));
        assertThrows(ConfigException.class, () -> trailingComma.getTableNameForTopic("other-topic"),
                "Trailing comma (empty entry) must throw when scanned");
    }

    @Test
    void testGetJdbcConfigWithAllValues() {
        JdbcConfig result = sinkConfig.getJdbcConfig();
        
        assertNotNull(result);
        assertEquals("jdbc:firebolt:test_db", result.getJdbcConnectionUrl());
        assertEquals(Optional.of("test_client_id"), result.getClientId());
        assertEquals(Optional.of("test_client_secret"), result.getClientSecret());
    }

    @Test
    void testGetJdbcConfigWithMissingOptionalValues() {
        configMap.remove(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG);
        configMap.remove(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        JdbcConfig result = testConfig.getJdbcConfig();
        
        assertNotNull(result);
        assertEquals("jdbc:firebolt:test_db", result.getJdbcConnectionUrl());
        assertEquals(Optional.empty(), result.getClientId());
        assertEquals(Optional.empty(), result.getClientSecret());
    }

    @Test
    void testGetJdbcConfigWithNullValues() {
        configMap.put(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG, null);
        configMap.put(ConnectorConfigDefinition.FIREBOLT_CLIENT_ID_CONFIG, null);
        configMap.put(ConnectorConfigDefinition.FIREBOLT_CLIENT_SECRET_CONFIG, null);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        JdbcConfig result = testConfig.getJdbcConfig();
        
        assertNotNull(result);
        assertNull(result.getJdbcConnectionUrl());
        assertEquals(Optional.empty(), result.getClientId());
        assertEquals(Optional.empty(), result.getClientSecret());
    }

    @ParameterizedTest
    @CsvSource({
        "test.key1, test.value1",
        "test.key2, test.value2",
        "complex.nested.key, complex.value",
        "empty.key, ''",
        "number.key, 12345"
    })
    void testGetUtilityMethod(String key, String expectedValue) {
        configMap.put(key, expectedValue);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.get(key);
        
        assertEquals(expectedValue, result);
    }

    @Test
    void testGetUtilityMethodWithNonexistentKey() {
        String result = sinkConfig.get("nonexistent.key");
        
        assertNull(result);
    }

    @Test
    void testGetUtilityMethodWithNullKey() {
        String result = sinkConfig.get(null);
        
        assertNull(result);
    }

    @Test
    void testGetConfigReturnsOriginalMap() {
        Map<String, String> result = sinkConfig.getConfig();
        
        assertNotNull(result);
        assertSame(configMap, result);
        assertEquals(configMap.size(), result.size());
        assertEquals("jdbc:firebolt:test_db", result.get(ConnectorConfigDefinition.JDBC_CONNECTION_URL_CONFIG));
    }

    @Test
    void testGetConfigReturnsSameReference() {
        Map<String, String> result1 = sinkConfig.getConfig();
        Map<String, String> result2 = sinkConfig.getConfig();
        
        assertSame(result1, result2);
        assertSame(configMap, result1);
    }

    @ParameterizedTest
    @CsvSource({
        "true, true",
        "false, false"
    })
    void testIsExactlyOnceWithBooleanValues(String value, boolean expected) {
        configMap.put(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG, value);
        SinkConfig testConfig = new SinkConfig(configMap);

        boolean result = testConfig.isExactlyOnce();

        assertEquals(expected, result);
    }

    @Test
    void testIsExactlyOnceWhenOmittedDefaultsToFalse() {
        configMap.remove(ConnectorConfigDefinition.EXACTLY_ONCE_MAPPING_CONFIG);
        SinkConfig testConfig = new SinkConfig(configMap);

        boolean result = testConfig.isExactlyOnce();

        assertFalse(result);
    }

    // =========================
    // POST-PROCESSING SCRIPT TESTS
    // =========================

    @Test
    void shouldReturnEmptyWhenNoPostProcessingConfig() {
        SinkConfig config = new SinkConfig(Map.of());
        Optional<String> result = config.getPostProcessingScript("any_table");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnEmptyWhenPostProcessingConfigIsBlank() {
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", "   "));
        Optional<String> result = config.getPostProcessingScript("any_table");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnScriptWhenUsingInlineScript() {
        String json = "{\"mappings\":[{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"}]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        Optional<String> result = config.getPostProcessingScript("orders");
        assertTrue(result.isPresent());
        assertEquals("UPDATE \"orders\" SET processed = true", result.get());
    }

    @Test
    void shouldReturnScriptFromFileWhenUsingScriptFile(@TempDir Path tempDir) throws IOException {
        // Create a temporary script file
        Path scriptFile = tempDir.resolve("test_script.sql");
        String scriptContent = "UPDATE \"items\" SET status = 'processed' WHERE created_at < NOW() - INTERVAL '1 day'";
        Files.writeString(scriptFile, scriptContent);

        String json = "{\"mappings\":[{\"table\":\"items\",\"scriptFile\":\"" + scriptFile.toString() + "\"}]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        Optional<String> result = config.getPostProcessingScript("items");
        assertTrue(result.isPresent());
        assertEquals(scriptContent, result.get());
    }

    @Test
    void shouldThrowExceptionWhenScriptFileDoesNotExist() {
        String json = "{\"mappings\":[{\"table\":\"items\",\"scriptFile\":\"/non/existent/file.sql\"}]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> config.getPostProcessingScript("items"));
        assertTrue(exception.getMessage().contains("Script file does not exist"));
    }

    @Test
    void shouldThrowExceptionWhenCannotReadScriptFile(@TempDir Path tempDir) throws IOException {
        // Create a file that cannot be read (simulate permission issue)
        Path scriptFile = tempDir.resolve("unreadable.sql");
        Files.writeString(scriptFile, "some content");
        // Make file unreadable (this might not work on all systems, but worth testing)
        scriptFile.toFile().setReadable(false);

        String json = "{\"mappings\":[{\"table\":\"items\",\"scriptFile\":\"" + scriptFile.toString() + "\"}]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> config.getPostProcessingScript("items"));
        assertTrue(exception.getMessage().contains("Failed to read script file"));
    }

    @Test
    void shouldReturnEmptyWhenTableNotFound() {
        String json = "{\"mappings\":[{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"}]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        Optional<String> result = config.getPostProcessingScript("nonexistent_table");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldHandleMixedMappingsWithScriptAndScriptFile(@TempDir Path tempDir) throws IOException {
        // Create a temporary script file
        Path scriptFile = tempDir.resolve("items_script.sql");
        String scriptContent = "DELETE FROM \"items\" WHERE stale = true";
        Files.writeString(scriptFile, scriptContent);

        String json = "{\"mappings\":[" +
                "{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"}," +
                "{\"table\":\"items\",\"scriptFile\":\"" + scriptFile.toString() + "\"}" +
                "]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        // Test script mapping
        Optional<String> ordersResult = config.getPostProcessingScript("orders");
        assertTrue(ordersResult.isPresent());
        assertEquals("UPDATE \"orders\" SET processed = true", ordersResult.get());

        // Test scriptFile mapping
        Optional<String> itemsResult = config.getPostProcessingScript("items");
        assertTrue(itemsResult.isPresent());
        assertEquals(scriptContent, itemsResult.get());
    }

    @Test
    void shouldHandleEmptyMappingsArray() {
        String json = "{\"mappings\":[]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        Optional<String> result = config.getPostProcessingScript("any_table");
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldHandleInvalidJson() {
        String json = "{\"mappings\":[{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"}"; // Missing closing brace
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        Optional<String> result = config.getPostProcessingScript("orders");
        assertTrue(result.isEmpty()); // Should return empty on JSON parsing error
    }

    @Test
    void shouldThrowExceptionWhenNeitherScriptNorScriptFileSpecified() {
        // This should not happen due to validation, but test defensive programming
        String json = "{\"mappings\":[{\"table\":\"orders\"}]}";
        SinkConfig config = new SinkConfig(Map.of("post.processing.script", json));

        // This will throw an IllegalStateException in getScriptContent
        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> config.getPostProcessingScript("orders"));
        assertTrue(exception.getMessage().contains("Neither script nor scriptFile is specified"));
    }


} 