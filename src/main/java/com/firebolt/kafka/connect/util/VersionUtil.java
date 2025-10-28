package com.firebolt.kafka.connect.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for reading version information from the version.properties file.
 * This centralizes the version reading logic to avoid duplication across the codebase.
 */
@Slf4j
public class VersionUtil {

    private static final String VERSION_PROPERTIES_FILE = "version.properties";
    private static final String VERSION_PROPERTY_KEY = "version";
    private static final String DEFAULT_VERSION = "unknown";

    /**
     * Gets the version of the connector from the version.properties file.
     * 
     * @return the version string, or "unknown" if the version cannot be determined
     */
    public static String getVersion() {
        try {
            Properties properties = new Properties();
            try (InputStream input = VersionUtil.class.getClassLoader().getResourceAsStream(VERSION_PROPERTIES_FILE)) {
                if (input != null) {
                    properties.load(input);
                    return properties.getProperty(VERSION_PROPERTY_KEY, DEFAULT_VERSION);
                }
            }
        } catch (IOException e) {
            log.warn("Failed to load version from properties file", e);
        }
        return DEFAULT_VERSION;
    }
}
