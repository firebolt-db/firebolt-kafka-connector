package com.firebolt.kafka.connect.utils;

public class JdbcConnectionParser {

    /**
     * Returns the database from the jdbc url
     */
    public static String getDatabase(String jdbcUrl) {
        // Use a URL with the same structure as the default but with a non-existing database
        return jdbcUrl.replace("jdbc:firebolt:","").split("\\?")[0];
    }
}
