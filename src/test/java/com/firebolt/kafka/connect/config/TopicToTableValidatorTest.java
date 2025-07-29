package com.firebolt.kafka.connect.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for TopicToTableValidator.
 */
class TopicToTableValidatorTest {

    private TopicToTableValidator validator;

    @BeforeEach
    void setUp() {
        validator = new TopicToTableValidator();
    }

    @Test
    void shouldAllowNullValues() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", null));
    }

    @Test
    void shouldValidateSingleMapping() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", "topic1:table1"));
    }

    @Test
    void shouldValidateMultipleMappings() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", "topic1:table1,topic2:table2,topic3:table3"));
    }

    @Test
    void shouldHandleWhitespaceCorrectly() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", " topic1 : table1 , topic2 : table2 "));
    }

    @Test
    void shouldNotThrowExceptionForEmptyMapping() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", ""));
    }

    @Test
    void shouldNotThrowExceptionForWhitespaceOnlyMapping() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", "   "));
    }

    @Test
    void shouldThrowExceptionForMissingColon() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", "topic1table1")
        );
        assertTrue(exception.getMessage().contains("Invalid mapping format"));
        assertTrue(exception.getMessage().contains("Expected format: 'topic:table'"));
    }

    @Test
    void shouldThrowExceptionForMultipleColons() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", "topic1:table1:extra")
        );
        assertTrue(exception.getMessage().contains("Invalid mapping format"));
        assertTrue(exception.getMessage().contains("Expected format: 'topic:table'"));
    }

    @Test
    void shouldThrowExceptionForEmptyTopicName() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", ":table1")
        );
        assertTrue(exception.getMessage().contains("Topic name cannot be empty"));
    }

    @Test
    void shouldThrowExceptionForEmptyTableName() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", "topic1:")
        );
        assertTrue(exception.getMessage().contains("Table name cannot be empty"));
    }

    @Test
    void shouldThrowExceptionForDuplicateTopics() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", "topic1:table1,topic1:table2")
        );
        assertTrue(exception.getMessage().contains("Duplicate topic 'topic1'"));
    }

    @Test
    void shouldThrowExceptionForDuplicateTables() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", "topic1:table1,topic2:table1")
        );
        assertTrue(exception.getMessage().contains("Duplicate table 'table1'"));
    }

    @Test
    void shouldThrowExceptionForEmptyMappingInList() {
        ConfigException exception = assertThrows(
            ConfigException.class,
            () -> validator.ensureValid("test.config", "topic1:table1,,topic2:table2")
        );
        assertTrue(exception.getMessage().contains("Empty mapping found"));
    }

    @Test
    void shouldHandleComplexValidMapping() {
        String complexMapping = "orders_topic:orders_table,customers_topic:customers_table,products_topic:products_table";
        assertDoesNotThrow(() -> validator.ensureValid("test.config", complexMapping));
    }

    @Test
    void shouldHaveMeaningfulToString() {
        String result = validator.toString();
        assertTrue(result.contains("Topic to table mapping validator"));
        assertTrue(result.contains("topic1:table1,topic2:table2"));
    }

    @Test
    void shouldDetectDuplicateTopicsWithDifferentCases() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", "Topic1:table1,topic1:table2"));
    }

    @Test
    void shouldDetectDuplicateTablesWithDifferentCases() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", "topic1:Table1,topic2:table1"));
    }

    @Test
    void shouldHandleSpecialCharactersInNames() {
        assertDoesNotThrow(() -> validator.ensureValid("test.config", "topic_1:table-1,topic.2:table_2"));
    }
} 