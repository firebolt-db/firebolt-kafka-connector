package com.firebolt.kafka.connect.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.PostProcessingConfig;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
            PostProcessingConfig postProcessingConfig = OBJECT_MAPPER.readValue(json, PostProcessingConfig.class);
            if (postProcessingConfig == null) {
                throw new ConfigException(name, value, "Post-processing must be a JSON object");
            }

            List<PostProcessingConfig.Mapping> mappings = postProcessingConfig.getMappings();
            if (!CollectionUtils.isEmpty(mappings)) {
                mappings.stream().forEach(mapping -> {
                    if (StringUtils.isBlank(mapping.getTable())) {
                        throw new ConfigException(name, value, "Each mapping requires non-empty 'table'");
                    }
                    
                    boolean hasScript = StringUtils.isNotBlank(mapping.getScript());
                    boolean hasScriptFile = StringUtils.isNotBlank(mapping.getScriptFile());
                    
                    if (!hasScript && !hasScriptFile) {
                        throw new ConfigException(name, value, "Each mapping requires either 'script' or 'scriptFile' to be specified");
                    }
                    
                    if (hasScript && hasScriptFile) {
                        throw new ConfigException(name, value, "Each mapping cannot have both 'script' and 'scriptFile' specified - use only one");
                    }
                });
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


