package com.firebolt.kafka.connect.config;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration definition for Firebolt Sink Connector.
 */
public class ConnectorConfigDefinition {

    // =========================
    // FIREBOLT JDBC CONNECTION CONFIGURATION
    // =========================
    public static final String JDBC_CONNECTION_URL_CONFIG = "jdbc.connection.url";
    public static final String JDBC_CONNECTION_URL_DOC = "Firebolt JDBC connection URL (e.g., jdbc:firebolt:my_database?engine=my_engine&account=my_account)";
    public static final String JDBC_CONNECTION_URL_DEFAULT = null;

    public static final String FIREBOLT_CLIENT_ID_CONFIG = "firebolt.clientId";
    public static final String FIREBOLT_CLIENT_ID_DOC = "The client id that will be used to connect to your Firebolt account";
    public static final String FIREBOLT_CLIENT_ID_DEFAULT = null;

    public static final String FIREBOLT_CLIENT_SECRET_CONFIG = "firebolt.clientSecret";
    public static final String FIREBOLT_CLIENT_SECRET_DOC = "The client secret that will be used to connect to your Firebolt account";
    public static final String FIREBOLT_CLIENT_SECRET_DEFAULT = null;

    // =========================
    // TABLE CONFIGURATION
    // =========================
    public static final String TOPIC_TO_TABLE_MAPPING_CONFIG = "topic.to.table.mapping";
    public static final String TOPIC_TO_TABLE_MAPPING_DOC = "Comma-separated mapping of Kafka topics to Firebolt table names (e.g., topic1:table1,topic2:table2)";
    public static final String TOPIC_TO_TABLE_MAPPING_DEFAULT = null;

    // =========================
    // ERROR HANDLING CONFIGURATION (delegated from Kafka Connect worker)
    // =========================
    public static final String ERROR_TOLERANCE_CONFIG = "errors.tolerance";
    public static final String ERROR_TOLERANCE_DOC = "Error tolerance policy. Supported values: 'none' (default) or 'all'. When 'all', errant records are reported to DLQ if configured.";
    public static final String ERROR_TOLERANCE_DEFAULT = "none";

    // =========================
    // CONFIG DEFINITION
    // =========================
    public static ConfigDef CONFIG_DEF = createConfigDef();

    /**
     * Creates the configuration definition for the Firebolt Sink Connector.
     * 
     * @return ConfigDef with all configuration properties defined
     */
    private static ConfigDef createConfigDef() {
        return new ConfigDef()
                // Connection Configuration
                .define(JDBC_CONNECTION_URL_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new JdbcConnectionUrlValidator(),
                        ConfigDef.Importance.HIGH,
                        JDBC_CONNECTION_URL_DOC)
                .define(FIREBOLT_CLIENT_ID_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        FIREBOLT_CLIENT_ID_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        FIREBOLT_CLIENT_ID_DOC)
                .define(FIREBOLT_CLIENT_SECRET_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        FIREBOLT_CLIENT_SECRET_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        FIREBOLT_CLIENT_SECRET_DOC)
                
                // Table Configuration
                .define(TOPIC_TO_TABLE_MAPPING_CONFIG,
                        ConfigDef.Type.STRING,
                        TOPIC_TO_TABLE_MAPPING_DEFAULT,
                        new TopicToTableValidator(),
                        ConfigDef.Importance.HIGH,
                        TOPIC_TO_TABLE_MAPPING_DOC)
                // Error handling configuration (optional; typically set at worker level but surfaced here for clarity/testing)
                .define(ERROR_TOLERANCE_CONFIG,
                        ConfigDef.Type.STRING,
                        ERROR_TOLERANCE_DEFAULT,
                        ConfigDef.ValidString.in("none", "all"),
                        ConfigDef.Importance.MEDIUM,
                        ERROR_TOLERANCE_DOC);
    }
} 