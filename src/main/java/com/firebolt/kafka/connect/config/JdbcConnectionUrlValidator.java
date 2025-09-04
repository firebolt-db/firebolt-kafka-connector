package com.firebolt.kafka.connect.config;

import java.net.URI;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Ensures the URL follows the correct format and starts with 'jdbc:firebolt:'.
 */
@Slf4j
class JdbcConnectionUrlValidator implements ConfigDef.Validator {

    private static final String JDBC_PREFIX = "jdbc:firebolt:";

    private static final Set<String> MANDATORY_CLOUD_JDBC_PARAMETERS = Set.of("database", "engine", "account");

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

        Properties jdbcProperties;
        try {
            jdbcProperties = parseJdbcConnectionString(url);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(name, value, "Invalid Firebolt JDBC connection string.");
        }

        // if there is the url property on the jdbc connection string, then consider this is a core connection
        if (!jdbcProperties.contains("url")) {

            for (String mandatoryParameter : MANDATORY_CLOUD_JDBC_PARAMETERS) {
                if (StringUtils.isBlank((String) jdbcProperties.get(mandatoryParameter))) {
                    log.error("The jdbc url does not have the {} parameter. When connecting to Firebolt Cloud this is mandatory.", mandatoryParameter);
                    throw new ConfigException(name, value, "The jdbc url does not have the " + mandatoryParameter + " parameter. When connecting to Firebolt Cloud this is mandatory.");
                }
            }
        }
    }

    @Override
    public String toString() {
        return "Firebolt JDBC URL validator";
    }

    /**
     * Return the jdbc connection string as parsed key value pairs
     * @param jdbcConnectionUrl
     * @return
     */
    private static Properties parseJdbcConnectionString(String jdbcConnectionUrl) {
        String cleanURI = jdbcConnectionUrl.replace(JDBC_PREFIX, "");
        URI uri = URI.create(cleanURI);
        Properties uriProperties = new Properties();
        String query = uri.getQuery();
        if (query != null && !query.isBlank()) {
            String[] queryKeyValues = query.split("&");
            for (String keyValue : queryKeyValues) {
                String[] keyValueTokens = keyValue.split("=");
                if (keyValueTokens.length == 2) {
                    uriProperties.put(keyValueTokens[0], keyValueTokens[1]);
                } else {
                    log.warn("Cannot parse key-pair: {}", keyValue);
                }
            }
        }

        uriProperties.put("database", uri.getPath());
        return uriProperties;
    }

}
