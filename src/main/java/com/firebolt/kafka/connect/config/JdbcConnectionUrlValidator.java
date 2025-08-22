package com.firebolt.kafka.connect.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Ensures the URL follows the correct format and starts with 'jdbc:firebolt:'.
 */
class JdbcConnectionUrlValidator implements ConfigDef.Validator {
    
    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            throw new ConfigException(name, value, "JDBC connection URL is required");
        }

        String url = value.toString().trim();
        if (url.isEmpty()) {
            throw new ConfigException(name, value, "JDBC connection URL cannot be empty");
        }
        
        if (!url.startsWith("jdbc:firebolt:")) {
            throw new ConfigException(name, value, 
                "Connection URL must start with 'jdbc:firebolt:'. Got: " + url);
        }
    }

    @Override
    public String toString() {
        return "Firebolt JDBC URL validator";
    }
}
