package com.firebolt.kafka.connect.utils;


import com.firebolt.kafka.connect.clients.FireboltClient;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.awaitility.Awaitility.await;

@Slf4j
public class ServiceHealthExtension implements BeforeAllCallback {
    
    private static final String KAFKA_CONNECT_HOST = "http://localhost:8083";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofMinutes(1);
    
    private static boolean healthCheckCompleted = false;
    private static final Object lock = new Object();
    
    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build();
    
    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (lock) {
            if (!healthCheckCompleted) {
                log.info("========================================");
                log.info("Starting Integration Test Suite Health Checks");
                log.info("========================================");

                checkKafkaHealth();
                checkFireboltHealth();
                checkSchemaRegistryHealth();
                checkKafkaConnectHealth();

                healthCheckCompleted = true;
                
                log.info("========================================");
                log.info("All Services are Healthy - Starting Tests");
                log.info("========================================");
            } else {
                log.debug("Health checks already completed, skipping for class: {}", 
                         context.getRequiredTestClass().getSimpleName());
            }
        }
    }
    
    private void checkKafkaConnectHealth() {
        log.info("Checking Kafka Connect health...");
        
        await("Kafka Connect to be healthy")
            .atMost(HEALTH_CHECK_TIMEOUT)
            .pollInterval(Duration.ofSeconds(5))
            .until(() -> {
                try {
                    Request request = new Request.Builder()
                            .url(KAFKA_CONNECT_HOST + "/")
                            .get()
                            .build();
                    
                    try (Response response = httpClient.newCall(request).execute()) {
                        if (response.isSuccessful()) {
                            String body = response.body().string();
                            log.debug("Kafka Connect response: {}", body);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    log.debug("Kafka Connect health check failed: {}", e.getMessage());
                }
                return false;
            });
        
        // Also check connector plugins are available
        await("Kafka Connect plugins to be available")
            .atMost(Duration.ofSeconds(30))
            .until(() -> {
                try {
                    Request request = new Request.Builder()
                            .url(KAFKA_CONNECT_HOST + "/connector-plugins")
                            .get()
                            .build();
                    
                    try (Response response = httpClient.newCall(request).execute()) {
                        if (response.isSuccessful()) {
                            String body = response.body().string();
                            log.debug("Available connector plugins: {}", body);
                            return body.contains("FireboltSinkConnector");
                        }
                    }
                } catch (Exception e) {
                    log.debug("Connector plugins check failed: {}", e.getMessage());
                }
                return false;
            });
        
        log.info("✅ Kafka Connect is healthy and FireboltSinkConnector is available");
    }
    
    private void checkKafkaHealth() {
        log.info("Checking Kafka health...");
        
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        
        await("Kafka to be healthy")
            .atMost(HEALTH_CHECK_TIMEOUT)
            .pollInterval(Duration.ofSeconds(5))
            .until(() -> {
                try (AdminClient adminClient = AdminClient.create(adminProps)) {
                    DescribeClusterResult clusterResult = adminClient.describeCluster();
                    String clusterId = clusterResult.clusterId().get(30, TimeUnit.SECONDS);
                    int nodeCount = clusterResult.nodes().get(30, TimeUnit.SECONDS).size();
                    
                    log.debug("Kafka cluster ID: {}, nodes: {}", clusterId, nodeCount);
                    return clusterId != null && nodeCount > 0;
                } catch (Exception e) {
                    log.debug("Kafka health check failed: {}", e.getMessage());
                    return false;
                }
            });
        
        // Also test producer connectivity
        await("Kafka producer connectivity")
            .atMost(Duration.ofSeconds(30))
            .until(() -> {
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
                
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                    // Just check that we can create a producer without errors
                    producer.partitionsFor("__consumer_offsets"); // This will test connectivity
                    return true;
                } catch (Exception e) {
                    log.debug("Kafka producer connectivity check failed: {}", e.getMessage());
                    return false;
                }
            });
        
        log.info("✅ Kafka is healthy and accepting connections");
    }
    
        private void checkFireboltHealth() {
        log.info("Checking Firebolt health...");
        
        await("Firebolt to be healthy")
            .atMost(HEALTH_CHECK_TIMEOUT)
            .pollInterval(Duration.ofSeconds(10))
            .until(() -> {
                try (FireboltClient client = FireboltClient.createDefault()) {
                    return client.testConnection();
                } catch (Exception e) {
                    log.debug("Firebolt health check failed: {}", e.getMessage());
                    return false;
                }
            });
        
        log.info("✅ Firebolt is healthy and accepting connections");
    }
    
    private void checkSchemaRegistryHealth() {
        log.info("Checking Schema Registry health...");
        
        await("Schema Registry to be healthy")
            .atMost(HEALTH_CHECK_TIMEOUT)
            .pollInterval(Duration.ofSeconds(5))
            .until(() -> {
                try {
                    Request request = new Request.Builder()
                            .url(SCHEMA_REGISTRY_URL + "/subjects")
                            .get()
                            .build();
                    
                    try (Response response = httpClient.newCall(request).execute()) {
                        if (response.isSuccessful()) {
                            String body = response.body().string();
                            log.debug("Schema Registry subjects: {}", body);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    log.debug("Schema Registry health check failed: {}", e.getMessage());
                }
                return false;
            });
        
        // Also check that we can interact with the registry
        await("Schema Registry API to be functional")
            .atMost(Duration.ofSeconds(30))
            .until(() -> {
                try {
                    Request request = new Request.Builder()
                            .url(SCHEMA_REGISTRY_URL + "/config")
                            .get()
                            .build();
                    
                    try (Response response = httpClient.newCall(request).execute()) {
                        if (response.isSuccessful()) {
                            String body = response.body().string();
                            log.debug("Schema Registry config: {}", body);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    log.debug("Schema Registry API check failed: {}", e.getMessage());
                }
                return false;
            });
        
        log.info("✅ Schema Registry is healthy and API is functional");
    }
} 