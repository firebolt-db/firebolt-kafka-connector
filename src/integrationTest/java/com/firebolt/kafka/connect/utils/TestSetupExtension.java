package com.firebolt.kafka.connect.utils;


import com.firebolt.kafka.connect.clients.FireboltClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that sets up integration test database before any integration tests run.
 * This extension runs once per test suite, similar to ServiceHealthExtension.
 */
@Slf4j
public class TestSetupExtension implements BeforeAllCallback {
    
    private static final String DATABASE_NAME = "integration_test_db";
    private static boolean databaseSetupCompleted = false;
    private static final Object lock = new Object();
    
    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (lock) {
            if (!databaseSetupCompleted) {
                log.info("========================================");
                log.info("Setting up Integration Test Database");
                log.info("========================================");
                
                setupDatabase();
                
                databaseSetupCompleted = true;
                
                log.info("========================================");
                log.info("Integration Test Database Setup Complete");
                log.info("========================================");
            } else {
                log.debug("Database setup already completed, skipping for class: {}", 
                         context.getRequiredTestClass().getSimpleName());
            }
        }
    }
    
        private void setupDatabase() {
        try {
            log.info("Creating integration test database...");
            
            try (FireboltClient client = FireboltClient.createDefault()) {
                // drop database if it exists so we start fresh every time
                client.dropDatabase(DATABASE_NAME);

                client.createDatabase(DATABASE_NAME);
                log.info("✅ Integration test database created successfully");
            }
            
        } catch (Exception e) {
            log.error("❌ Failed to setup integration test database: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to setup integration test database", e);
        }
    }
} 