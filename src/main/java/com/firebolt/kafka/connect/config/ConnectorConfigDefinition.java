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

    // Optional post processing SQL script(s) to run per table after insert
    // JSON format: { "mappings" : [ { "table" : "<table>", "script" : "<sql>" } ] }
    public static final String POST_PROCESSING_SCRIPT_CONFIG = "post.processing.script";
    public static final String POST_PROCESSING_SCRIPT_DOC = "Optional post-processing SQL to run after insert per table. JSON format: {\"mappings\":[{\"table\":\"<table>\",\"script\":\"<sql>\"}]} or {\"mappings\":[{\"table\":\"<table>\",\"scriptFile\":\"<path/to/script.sql>\"}]}. Either 'script' or 'scriptFile' must be specified for each mapping, but not both.";
    public static final String POST_PROCESSING_SCRIPT_DEFAULT = null;

    // =========================
    // CONNECTOR BEHAVIOR
    // =========================
    public static final String EXACTLY_ONCE_MAPPING_CONFIG = "exactlyOnce";
    public static final String EXACTLY_ONCE_MAPPING_DOC = "By default this will be set to false. When set to true, then the kafka message will be ingested exactly-once in Firebolt. When the flag is false, the kafka message will be ingested at least once";
    public static final Boolean EXACTLY_ONCE_MAPPING_DEFAULT = Boolean.FALSE;

    // =========================
    // INGESTION MODE
    // =========================
    public static final String INGESTION_TYPE_CONFIG = "ingestion.type";
    public static final String INGESTION_TYPE_DOC = "Deprecated and ignored. Records are uploaded over upload:// and ingested server-side via read_avro (schema-carrying records) or read_json (schemaless JSON). Accepted for backwards compatibility with existing connector configs.";
    public static final String INGESTION_TYPE_DEFAULT = "parquet";

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
                // Connector behavior
                .define(EXACTLY_ONCE_MAPPING_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        EXACTLY_ONCE_MAPPING_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        EXACTLY_ONCE_MAPPING_DOC)
                .define(INGESTION_TYPE_CONFIG,
                        ConfigDef.Type.STRING,
                        INGESTION_TYPE_DEFAULT,
                        ConfigDef.ValidString.in("sql", "binary", "parquet"),
                        ConfigDef.Importance.LOW,
                        INGESTION_TYPE_DOC)

                // Table Configuration
                .define(TOPIC_TO_TABLE_MAPPING_CONFIG,
                        ConfigDef.Type.STRING,
                        TOPIC_TO_TABLE_MAPPING_DEFAULT,
                        new TopicToTableValidator(),
                        ConfigDef.Importance.HIGH,
                        TOPIC_TO_TABLE_MAPPING_DOC)
                .define(POST_PROCESSING_SCRIPT_CONFIG,
                        ConfigDef.Type.STRING,
                        POST_PROCESSING_SCRIPT_DEFAULT,
                        new PostProcessingScriptValidator(),
                        ConfigDef.Importance.MEDIUM,
                        POST_PROCESSING_SCRIPT_DOC)
                // Error handling configuration (optional; typically set at worker level but surfaced here for clarity/testing)
                .define(ERROR_TOLERANCE_CONFIG,
                        ConfigDef.Type.STRING,
                        ERROR_TOLERANCE_DEFAULT,
                        ConfigDef.ValidString.in("none", "all"),
                        ConfigDef.Importance.MEDIUM,
                        ERROR_TOLERANCE_DOC);
    }
} 