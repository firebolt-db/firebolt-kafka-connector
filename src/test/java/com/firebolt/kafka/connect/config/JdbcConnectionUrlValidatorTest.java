package com.firebolt.kafka.connect.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for JdbcConnectionUrlValidator.
 */
class JdbcConnectionUrlValidatorTest {

    private final JdbcConnectionUrlValidator validator = new JdbcConnectionUrlValidator();

    @Test
    void shouldRejectNullValues() {
        ConfigException exception = assertThrows(ConfigException.class,
            () -> validator.ensureValid("test", null));
        assertTrue(exception.getMessage().contains("JDBC connection URL is required"));
    }

    @Test
    void shouldAcceptValidFireboltJdbcUrl() {
        String validUrl = "jdbc:firebolt:my_database?engine=my_engine&account=my_account";
        assertDoesNotThrow(() -> validator.ensureValid("test", validUrl));
    }

    @Test
    void shouldRejectEmptyUrl() {
        ConfigException exception = assertThrows(ConfigException.class,
            () -> validator.ensureValid("test", ""));
        assertTrue(exception.getMessage().contains("JDBC connection URL cannot be empty"));
    }

    @Test
    void shouldRejectWhitespaceOnlyUrl() {
        ConfigException exception = assertThrows(ConfigException.class,
            () -> validator.ensureValid("test", "   "));
        assertTrue(exception.getMessage().contains("JDBC connection URL cannot be empty"));
    }

    @Test
    void shouldRejectInvalidPrefix() {
        String invalidUrl = "jdbc:postgresql://localhost:5432/db";
        ConfigException exception = assertThrows(ConfigException.class,
            () -> validator.ensureValid("test", invalidUrl));
        assertTrue(exception.getMessage().contains("Connection URL must start with 'jdbc:firebolt:'"));
    }

    @Test
    void shouldAcceptUrlWithTrimmedWhitespace() {
        String validUrl = "  jdbc:firebolt:my_database?engine=my_engine&account=my_account  ";
        assertDoesNotThrow(() -> validator.ensureValid("test", validUrl));
    }
} 