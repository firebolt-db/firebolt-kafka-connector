package com.firebolt.kafka.connect.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostProcessingScriptValidatorTest {

    private final PostProcessingScriptValidator validator = new PostProcessingScriptValidator();

    @Test
    void shouldAllowNullValue() {
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", null));
    }

    @Test
    void shouldAllowEmptyString() {
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", ""));
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", "   "));
    }

    @Test
    void shouldRejectInvalidJson() {
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", "{ invalid json"));
    }

    @Test
    void shouldRejectNonObjectJson() {
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", "[]"));
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", "\"string\""));
    }

    @Test
    void shouldAllowMissingMappings() {
        String json = "{}";
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", json));
    }

    @Test
    void shouldRejectNonArrayButAllowEmptyArray() {
        String nonArray = "{\"mappings\":{}}";
        String emptyArray = "{\"mappings\":[]}";
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", nonArray));
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", emptyArray));
    }

    @Test
    void shouldRejectMappingThatIsNotObject() {
        String json = "{\"mappings\":[\"not-an-object\"]}";
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", json));
    }

    @Test
    void shouldRejectMissingOrEmptyTable() {
        String missingTable = "{\"mappings\":[{\"script\":\"UPDATE t SET c=1\"}]}";
        String emptyTable = "{\"mappings\":[{\"table\":\"  \",\"script\":\"UPDATE t SET c=1\"}]}";
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", missingTable));
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", emptyTable));
    }

    @Test
    void shouldRejectMissingOrEmptyScript() {
        String missingScript = "{\"mappings\":[{\"table\":\"t\"}]}";
        String emptyScript = "{\"mappings\":[{\"table\":\"t\",\"script\":\"   \"}]}";
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", missingScript));
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", emptyScript));
    }

    @Test
    void shouldRejectMissingBothScriptAndScriptFile() {
        String missingBoth = "{\"mappings\":[{\"table\":\"t\"}]}";
        String emptyBoth = "{\"mappings\":[{\"table\":\"t\",\"script\":\"   \",\"scriptFile\":\"   \"}]}";
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", missingBoth));
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", emptyBoth));
    }

    @Test
    void shouldRejectBothScriptAndScriptFile() {
        String bothSpecified = "{\"mappings\":[{\"table\":\"t\",\"script\":\"UPDATE t SET c=1\",\"scriptFile\":\"/path/to/script.sql\"}]}";
        assertThrows(ConfigException.class, () -> validator.ensureValid("post.processing.script", bothSpecified));
    }

    @Test
    void shouldAcceptScriptFileOnly() {
        String scriptFileOnly = "{\"mappings\":[{\"table\":\"t\",\"scriptFile\":\"/path/to/script.sql\"}]}";
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", scriptFileOnly));
    }

    @Test
    void shouldAcceptScriptOnly() {
        String scriptOnly = "{\"mappings\":[{\"table\":\"t\",\"script\":\"UPDATE t SET c=1\"}]}";
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", scriptOnly));
    }

    @Test
    void shouldAcceptValidSingleMapping() {
        String json = "{\"mappings\":[{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"}]}";
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", json));
    }

    @Test
    void shouldAcceptValidMultipleMappings() {
        String json = "{\"mappings\":[{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"},{\"table\":\"items\",\"script\":\"DELETE FROM \\\"items\\\" WHERE stale = true\"}]}";
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", json));
    }

    @Test
    void shouldAcceptMixedMappingsWithScriptAndScriptFile() {
        String json = "{\"mappings\":[{\"table\":\"orders\",\"script\":\"UPDATE \\\"orders\\\" SET processed = true\"},{\"table\":\"items\",\"scriptFile\":\"/path/to/items_script.sql\"}]}";
        assertDoesNotThrow(() -> validator.ensureValid("post.processing.script", json));
    }
}


