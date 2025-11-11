package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

    @ParameterizedTest
    @CsvSource({
        "topic1, table1",
        "topic2, table2"
    })
    void testGetTableNameForTopicWithValidMappings(String topic, String expectedTable) {
        String result = sinkConfig.getTableNameForTopic(topic);
        
        assertEquals(expectedTable, result);
    }

    @Test
    void testGetTableNameForTopicWithSingleMapping() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "single-topic:single_table");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTableNameForTopic("single-topic");
        
        assertEquals("single_table", result);
    }

    @Test
    void testGetTableNameForTopicWithComplexMapping() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, 
                "user-events:user_events_table,order-data:orders_table,metrics:metrics_table");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        assertEquals("user_events_table", testConfig.getTableNameForTopic("user-events"));
        assertEquals("orders_table", testConfig.getTableNameForTopic("order-data"));
        assertEquals("metrics_table", testConfig.getTableNameForTopic("metrics"));
    }

    @Test
    void testGetTableNameForTopicWithSpacesInMapping() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, 
                " topic1 : table1 , topic2 : table2 ");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        assertEquals("table1", testConfig.getTableNameForTopic("topic1"));
        assertEquals("table2", testConfig.getTableNameForTopic("topic2"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "topic3",
        "nonexistent-topic",
        "TOPIC1",
        "topic1_different",
        ""
    })
    void testGetTableNameForTopicWithNonexistentTopic(String topic) {
        String result = sinkConfig.getTableNameForTopic(topic);
        
        assertNull(result);
    }

    @Test
    void testGetTableNameForTopicWithNullMapping() {
        configMap.remove(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTableNameForTopic("any-topic");
        
        assertNull(result);
    }

    @Test
    void testGetTableNameForTopicWithEmptyMapping() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTableNameForTopic("any-topic");
        
        assertNull(result);
    }

    @Test
    void testGetTableNameForTopicWithWhitespaceOnlyMapping() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "   ");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTableNameForTopic("any-topic");
        
        assertNull(result);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "topic1",
        "topic1:table1:extra",
        "topic1:",
        ":table1",
        "topic1:table1,malformed",
        "topic1:table1,:",
        "topic1:table1,"
    })
    void testGetTableNameForTopicWithMalformedMappings(String malformedMapping) {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, malformedMapping);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTableNameForTopic("topic1");
        
        // Should either return correct value or null for malformed entries
        if (malformedMapping.equals("topic1:table1:extra")) {
            assertNull(result); // parts.length != 2
        } else if (malformedMapping.equals("topic1:")) {
            assertNull(result); // empty table name
        }
        // Other cases should handle gracefully
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
        "'topic1:table1', topic1, table1",
        "'user-events:user_table', user-events, user_table",
        "'metrics_topic:metrics_db_table', metrics_topic, metrics_db_table",
        "'test.topic:test.table', test.topic, test.table",
        "'topic-with-dashes:table_with_underscores', topic-with-dashes, table_with_underscores"
    })
    void testGetTableNameForTopicWithVariousNamingConventions(String mapping, String topic, String expectedTable) {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, mapping);
        SinkConfig testConfig = new SinkConfig(configMap);
        
        String result = testConfig.getTableNameForTopic(topic);
        
        assertEquals(expectedTable, result);
    }

    @Test
    void testGetTableNameForTopicCaseSensitive() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "Topic1:Table1,topic2:table2");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        assertEquals("Table1", testConfig.getTableNameForTopic("Topic1"));
        assertEquals("table2", testConfig.getTableNameForTopic("topic2"));
        assertNull(testConfig.getTableNameForTopic("topic1")); // Case sensitive
        assertNull(testConfig.getTableNameForTopic("TOPIC2")); // Case sensitive
    }

    @Test
    void testGetTableNameForTopicWithDuplicateTopics() {
        configMap.put(ConnectorConfigDefinition.TOPIC_TO_TABLE_MAPPING_CONFIG, "topic1:table1,topic1:table2");
        SinkConfig testConfig = new SinkConfig(configMap);
        
        // Should return the first match
        String result = testConfig.getTableNameForTopic("topic1");
        
        assertEquals("table1", result);
    }

    @Test
    void testGetTableNameForTopicWithEmptyTopicName() {
        String result = sinkConfig.getTableNameForTopic("");
        
        assertNull(result);
    }

    @Test
    void testGetTableNameForTopicWithNullTopicName() {
        String result = sinkConfig.getTableNameForTopic(null);
        
        assertNull(result);
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

    @ParameterizedTest
    @CsvSource({
        "true, true",
        "false, false"
    })
    void testIsOptimizeInsertsWithBooleanValues(String value, boolean expected) {
        configMap.put(ConnectorConfigDefinition.OPTIMIZE_INSERTS_CONFIG, value);
        SinkConfig testConfig = new SinkConfig(configMap);

        boolean result = testConfig.isOptimizeInserts();

        assertEquals(expected, result);
    }

    @Test
    void testIsOptimizeInsertsWhenOmittedDefaultsToFalse() {
        configMap.remove(ConnectorConfigDefinition.OPTIMIZE_INSERTS_CONFIG);
        SinkConfig testConfig = new SinkConfig(configMap);

        boolean result = testConfig.isOptimizeInserts();

        assertFalse(result);
    }

    @Test
    void testIsOptimizeInsertsWhenNullDefaultsToFalse() {
        configMap.put(ConnectorConfigDefinition.OPTIMIZE_INSERTS_CONFIG, null);
        SinkConfig testConfig = new SinkConfig(configMap);

        boolean result = testConfig.isOptimizeInserts();

        assertFalse(result);
    }

    
} 