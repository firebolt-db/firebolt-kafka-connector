package com.firebolt.kafka.connect;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/** The connector version, read from {@code version.properties} (written by the build). */
@Slf4j
final class Version {

    private Version() {
    }

    static String get() {
        try (InputStream input = Version.class.getClassLoader().getResourceAsStream("version.properties")) {
            if (input != null) {
                Properties properties = new Properties();
                properties.load(input);
                return properties.getProperty("version", "unknown");
            }
        } catch (IOException e) {
            log.warn("Failed to load version from properties file", e);
        }
        return "unknown";
    }
}
