package com.firebolt.kafka.connect.config;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration definition for Firebolt Sink Connector.
 */
public class ConnectorConfigDefinition {
    
    // TO BE populated

    // =========================
    // CONFIG DEFINITION
    // =========================
    public static ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        return new ConfigDef();
    }
} 