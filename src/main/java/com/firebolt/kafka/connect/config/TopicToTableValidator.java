package com.firebolt.kafka.connect.config;

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validator for topic to table mapping configuration.
 * Validates that the mapping is in the format "topic1:table1,topic2:table2"
 * and ensures no duplicate topics or tables.
 */
public class TopicToTableValidator implements ConfigDef.Validator {
    
    @Override
    public void ensureValid(String name, Object value) {
        // Allow null values during ConfigDef creation (default values)
        if (value == null) {
            return;
        }
        
        String mapping = value.toString().trim();
        
        // Allow empty values (will use default or be handled elsewhere)
        // if there is no mapping, we will use the topic name as the table name
        if (mapping.isEmpty()) {
            return;
        }

        Set<String> topics = new HashSet<>();
        Set<String> tables = new HashSet<>();
        
        String[] mappings = mapping.split(",");
        
        for (String mappingPair : mappings) {
            String trimmedPair = mappingPair.trim();
            
            if (trimmedPair.isEmpty()) {
                throw new ConfigException(name, value, "Empty mapping found in topic to table configuration");
            }
            
            String[] parts = trimmedPair.split(":", -1); // -1 to keep trailing empty strings
            
            if (parts.length != 2) {
                throw new ConfigException(name, value, 
                    "Invalid mapping format '" + trimmedPair + "'. Expected format: 'topic:table'");
            }
            
            String topic = parts[0].trim();
            String table = parts[1].trim();
            
            if (topic.isEmpty()) {
                throw new ConfigException(name, value, 
                    "Topic name cannot be empty in mapping '" + trimmedPair + "'");
            }
            
            if (table.isEmpty()) {
                throw new ConfigException(name, value, 
                    "Table name cannot be empty in mapping '" + trimmedPair + "'");
            }
            
            // Check for duplicate topics
            if (!topics.add(topic)) {
                throw new ConfigException(name, value, 
                    "Duplicate topic '" + topic + "' found in topic to table mapping");
            }
            
            // Check for duplicate tables
            if (!tables.add(table)) {
                throw new ConfigException(name, value, 
                    "Duplicate table '" + table + "' found in topic to table mapping");
            }
        }
    }
    
    @Override
    public String toString() {
        return "Topic to table mapping validator (format: topic1:table1,topic2:table2)";
    }
}