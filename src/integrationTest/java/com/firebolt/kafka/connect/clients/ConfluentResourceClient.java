package com.firebolt.kafka.connect.clients;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Client for Confluent Cloud Resource Management APIs.
 * Supports retrieving environment-level information like Schema Registry URLs.
 */
@Slf4j
public class ConfluentResourceClient implements AutoCloseable {

    private static final String DEFAULT_API_BASE = "https://api.confluent.cloud";

    private final String basicAuthHeader;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiBaseUrl;

    public ConfluentResourceClient(String apiKey, String apiSecret) {
        this(apiKey, apiSecret, DEFAULT_API_BASE,
            new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build(),
            new ObjectMapper());
    }

    public ConfluentResourceClient(String apiKey,
                                   String apiSecret,
                                   String apiBaseUrl,
                                   OkHttpClient httpClient,
                                   ObjectMapper objectMapper) {
        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            throw new IllegalArgumentException("Confluent Cloud API key and secret must be provided");
        }
        this.apiBaseUrl = apiBaseUrl == null || apiBaseUrl.isEmpty() ? DEFAULT_API_BASE : apiBaseUrl;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.basicAuthHeader = buildBasicAuthHeader(apiKey, apiSecret);
    }

    /**
     * Gets the Schema Registry URL for a specific environment.
     * Uses the Stream Catalog API v3 to find the Schema Registry cluster endpoint.
     */
    public String getSchemaRegistryUrl(String environmentId) throws IOException {
        if (environmentId == null || environmentId.isEmpty()) {
            throw new IllegalArgumentException("environmentId must be provided");
        }

        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("srcm/v3/clusters")
            .addQueryParameter("environment", environmentId)
            .build();

        Request request = new Request.Builder()
            .url(url)
            .get()
            .addHeader("Authorization", basicAuthHeader)
            .addHeader("Accept", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("get schema registry URL", response.code(), body);
            }

            // Minimal, dependency-free extraction:
            // Look for either "http_endpoint":"..." (v3 uses snake_case) or "httpEndpoint":"..." (seen in some docs/tools)
            String[] markers = { "\"http_endpoint\":\"", "\"httpEndpoint\":\"", "\"rest_endpoint\":\"" };
            for (String marker : markers) {
                int i = body.indexOf(marker);
                if (i >= 0) {
                    int start = i + marker.length();
                    int end = body.indexOf("\"", start);
                    if (end > start) {
                        return body.substring(start, end);
                    }
                }
            }
            
            throw new IOException("Schema Registry endpoint not found in response for environment: " + environmentId + ". Response: " + body);
        }
    }



    /**
     * Gets the Kafka cluster REST endpoint URL for a specific cluster ID and environment.
     * Uses the CMK API to find the cluster's REST endpoint.
     */
    public String getClusterEndpointUrl(String clusterId, String environmentId) throws IOException {
        if (clusterId == null || clusterId.isEmpty()) {
            throw new IllegalArgumentException("clusterId must be provided");
        }
        if (environmentId == null || environmentId.isEmpty()) {
            throw new IllegalArgumentException("environmentId must be provided");
        }

        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("cmk/v2/clusters")
            .addPathSegment(clusterId)
            .addQueryParameter("environment", environmentId)
            .build();

        Request request = new Request.Builder()
            .url(url)
            .get()
            .addHeader("Authorization", basicAuthHeader)
            .addHeader("Accept", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("get cluster endpoint URL", response.code(), body);
            }

            // Look for the REST endpoint in the response
            // Common field names: "rest_endpoint", "bootstrap_endpoint", "http_endpoint"
            String[] markers = { 
                "\"rest_endpoint\":\"", 
                "\"bootstrap_endpoint\":\"", 
                "\"http_endpoint\":\"",
                "\"restEndpoint\":\"",
                "\"bootstrapEndpoint\":\"",
                "\"httpEndpoint\":\""
            };
            
            for (String marker : markers) {
                int i = body.indexOf(marker);
                if (i >= 0) {
                    int start = i + marker.length();
                    int end = body.indexOf("\"", start);
                    if (end > start) {
                        String endpoint = body.substring(start, end);
                        // For bootstrap endpoints, extract just the host part if it includes port
                        if (marker.contains("bootstrap") && endpoint.contains(":")) {
                            // Convert from "pkc-xxxxx.region.aws.confluent.cloud:9092" 
                            // to "https://pkc-xxxxx.region.aws.confluent.cloud:443"
                            String host = endpoint.split(":")[0];
                            return "https://" + host + ":443";
                        }
                        return endpoint;
                    }
                }
            }
            
            throw new IOException("Cluster endpoint not found in response for cluster: " + clusterId + ". Response: " + body);
        }
    }

    /**
     * Gets the Kafka cluster Bootstrap server URL for a specific cluster ID and environment.
     * Uses the CMK API to find the cluster's Bootstrap endpoint.
     */
    public String getBootstrapServerUrl(String clusterId, String environmentId) throws IOException {
        if (clusterId == null || clusterId.isEmpty()) {
            throw new IllegalArgumentException("clusterId must be provided");
        }
        if (environmentId == null || environmentId.isEmpty()) {
            throw new IllegalArgumentException("environmentId must be provided");
        }

        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("cmk/v2/clusters")
            .addPathSegment(clusterId)
            .addQueryParameter("environment", environmentId)
            .build();

        Request request = new Request.Builder()
            .url(url)
            .get()
            .addHeader("Authorization", basicAuthHeader)
            .addHeader("Accept", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("get bootstrap server URL", response.code(), body);
            }

            // Look for the Bootstrap endpoint in the response
            // Common field names: "bootstrap_endpoint", "bootstrap_server_url", "kafka_bootstrap_endpoint"
            String[] markers = { 
                "\"bootstrap_endpoint\":\"", 
                "\"bootstrap_server_url\":\"", 
                "\"kafka_bootstrap_endpoint\":\"",
                "\"bootstrapEndpoint\":\"",
                "\"bootstrapServerUrl\":\"",
                "\"kafkaBootstrapEndpoint\":\""
            };
            
            for (String marker : markers) {
                int i = body.indexOf(marker);
                if (i >= 0) {
                    int start = i + marker.length();
                    int end = body.indexOf("\"", start);
                    if (end > start) {
                        return body.substring(start, end);
                    }
                }
            }
            
            throw new IOException("Bootstrap server endpoint not found in response for cluster: " + clusterId + ". Response: " + body);
        }
    }

    private void handleError(String operation, int statusCode, String responseBody) throws IOException {
        String message = String.format("Failed to %s (HTTP %d): %s", operation, statusCode, responseBody);
        
        if (statusCode == 401 || statusCode == 403) {
            message = String.format("Unauthorized to %s. Verify API key/secret and permissions. HTTP %d: %s", 
                operation, statusCode, responseBody);
        } else if (statusCode == 404) {
            message = String.format("Resource not found for %s. HTTP %d: %s", operation, statusCode, responseBody);
        }
        
        log.error(message);
        throw new IOException(message);
    }

    private static String buildBasicAuthHeader(String apiKey, String apiSecret) {
        String credentials = apiKey + ":" + apiSecret;
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    @Override
    public void close() {
        if (httpClient != null) {
            try {
                // Shutdown the executor service with timeout
                ExecutorService executor = httpClient.dispatcher().executorService();
                executor.shutdown();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Executor service did not terminate");
                    }
                }
                httpClient.connectionPool().evictAll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while shutting down HTTP client", e);
            }
        }
    }
}
