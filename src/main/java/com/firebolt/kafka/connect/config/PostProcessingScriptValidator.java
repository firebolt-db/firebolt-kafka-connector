package com.firebolt.kafka.connect.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validates the post-processing script configuration.
 * Expected JSON format:
 * { "mappings" : [ { "table" : "<table>", "script" : "<sql>" } ] }
 */
public class PostProcessingScriptValidator implements ConfigDef.Validator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            return; // optional config
        }

        String json = value.toString().trim();
        if (json.isEmpty()) {
            return; // treat empty as absent
        }

        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);
            if (root == null || !root.isObject()) {
                throw new ConfigException(name, value, "Post-processing must be a JSON object");
            }

            JsonNode mappings = root.get("mappings");
            if (mappings == null || !mappings.isArray() || mappings.size() == 0) {
                throw new ConfigException(name, value, "'mappings' must be a non-empty array");
            }

            for (JsonNode mapping : mappings) {
                if (!mapping.isObject()) {
                    throw new ConfigException(name, value, "Each mapping must be an object with 'table' and 'script'");
                }
                JsonNode table = mapping.get("table");
                JsonNode script = mapping.get("script");
                if (table == null || !table.isTextual() || table.asText().trim().isEmpty()) {
                    throw new ConfigException(name, value, "Each mapping requires non-empty 'table'");
                }
                if (script == null || !script.isTextual() || script.asText().trim().isEmpty()) {
                    throw new ConfigException(name, value, "Each mapping requires non-empty 'script'");
                }
            }
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(name, value, "Invalid JSON for post-processing script: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "Post-processing script validator (JSON format with mappings array)";
    }
}


