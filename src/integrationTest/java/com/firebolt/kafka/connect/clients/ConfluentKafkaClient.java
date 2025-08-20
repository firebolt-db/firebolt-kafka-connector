package com.firebolt.kafka.connect.clients;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Minimal client for Confluent Cloud Kafka REST API v3 to list topics in a cluster.
 */
@Slf4j
public class ConfluentKafkaClient implements AutoCloseable {

    private final String restEndpointBaseUrl; // e.g. https://pkc-xxxxx.region.provider.confluent.cloud
    private final String clusterId; // Kafka cluster id (starts with lkc-)
    private final String basicAuthHeader;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public ConfluentKafkaClient(String restEndpointBaseUrl,
                                String clusterId,
                                String apiKey,
                                String apiSecret) {
        this(restEndpointBaseUrl, clusterId, apiKey, apiSecret,
            new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build(),
            new ObjectMapper());
    }

    public ConfluentKafkaClient(String restEndpointBaseUrl,
                                String clusterId,
                                String apiKey,
                                String apiSecret,
                                OkHttpClient httpClient,
                                ObjectMapper objectMapper) {
        if (restEndpointBaseUrl == null || restEndpointBaseUrl.isEmpty()) {
            throw new IllegalArgumentException("restEndpointBaseUrl must be provided");
        }
        if (clusterId == null || clusterId.isEmpty()) {
            throw new IllegalArgumentException("clusterId must be provided");
        }
        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            throw new IllegalArgumentException("Confluent Cloud API key and secret must be provided");
        }
        this.restEndpointBaseUrl = restEndpointBaseUrl;
        this.clusterId = clusterId;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.basicAuthHeader = buildBasicAuthHeader(apiKey, apiSecret);
    }

    /**
     * Creates a new topic with the provided configuration. By default replication factor is set to 3.
     * Some Confluent Cloud cluster types enforce replication factor; adjust if needed via the overload.
     */
    public void createTopic(String topicName,
                            int partitionsCount,
                            Long retentionMs,
                            Long retentionBytes) throws IOException {
        createTopic(topicName, partitionsCount, (short) 3, retentionMs, retentionBytes);
    }

    /**
     * Creates a new topic with explicit replication factor and optional retention configs.
     */
    public void createTopic(String topicName,
                            int partitionsCount,
                            short replicationFactor,
                            Long retentionMs,
                            Long retentionBytes) throws IOException {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("topicName must be provided");
        }
        if (partitionsCount <= 0) {
            throw new IllegalArgumentException("partitionsCount must be > 0");
        }

        HttpUrl url = HttpUrl.parse(restEndpointBaseUrl)
            .newBuilder()
            .addPathSegments("kafka/v3/clusters")
            .addPathSegment(clusterId)
            .addPathSegment("topics")
            .build();

        // Build payload
        String jsonPayload = buildCreateTopicJson(topicName, partitionsCount, replicationFactor, retentionMs, retentionBytes);

        RequestBody body = RequestBody.create(jsonPayload, MediaType.parse("application/json"));
        Request request = new Request.Builder()
            .url(url)
            .post(body)
            .addHeader("Authorization", basicAuthHeader)
            .addHeader("Accept", "application/json")
            .addHeader("Content-Type", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String respBody = response.body() != null ? response.body().string() : "";
            if (response.isSuccessful() || response.code() == 201) {
                return;
            }
            int code = response.code();
            if (code == 409) {
                throw new IOException("Topic already exists: " + topicName + " (HTTP 409)");
            }
            if (code == 401 || code == 403) {
                throw new IOException("Unauthorized to create topic. Verify Kafka API key/secret and RBAC permissions. HTTP " + code);
            }
            throw new IOException("Failed to create topic (HTTP " + code + "): " + respBody);
        }
    }

    private String buildCreateTopicJson(String topicName,
                                        int partitionsCount,
                                        short replicationFactor,
                                        Long retentionMs,
                                        Long retentionBytes) throws IOException {
        // Build JSON structure per Kafka REST v3
        com.fasterxml.jackson.databind.node.ObjectNode root = objectMapper.createObjectNode();
        root.put("topic_name", topicName);
        root.put("partitions_count", partitionsCount);
        root.put("replication_factor", replicationFactor);

        com.fasterxml.jackson.databind.node.ArrayNode configs = objectMapper.createArrayNode();
        if (retentionMs != null) {
            com.fasterxml.jackson.databind.node.ObjectNode cfg = objectMapper.createObjectNode();
            cfg.put("name", "retention.ms");
            cfg.put("value", String.valueOf(retentionMs));
            configs.add(cfg);
        }
        if (retentionBytes != null) {
            com.fasterxml.jackson.databind.node.ObjectNode cfg = objectMapper.createObjectNode();
            cfg.put("name", "retention.bytes");
            cfg.put("value", String.valueOf(retentionBytes));
            configs.add(cfg);
        }
        if (configs.size() > 0) {
            root.set("configs", configs);
        }
        return objectMapper.writeValueAsString(root);
    }

    /**
     * Deletes a topic. In Kafka, deleting a topic removes all of its messages.
     */
    public void deleteTopic(String topicName) throws IOException {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("topicName must be provided");
        }

        HttpUrl url = HttpUrl.parse(restEndpointBaseUrl)
            .newBuilder()
            .addPathSegments("kafka/v3/clusters")
            .addPathSegment(clusterId)
            .addPathSegment("topics")
            .addPathSegment(topicName)
            .build();

        Request request = new Request.Builder()
            .url(url)
            .delete()
            .addHeader("Authorization", basicAuthHeader)
            .addHeader("Accept", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String respBody = response.body() != null ? response.body().string() : "";
            if (response.isSuccessful() || response.code() == 204) {
                return;
            }
            int code = response.code();
            if (code == 404) {
                throw new IOException("Topic not found: " + topicName + " (HTTP 404)");
            }
            if (code == 401 || code == 403) {
                throw new IOException("Unauthorized to delete topic. Verify Kafka API key/secret and RBAC permissions. HTTP " + code);
            }
            throw new IOException("Failed to delete topic (HTTP " + code + "): " + respBody);
        }
    }

    /**
     * Lists topic names in the cluster using Kafka REST API v3.
     * Follows pagination until all topics are retrieved.
     */
    public List<String> listTopics() throws IOException {
        List<String> topics = new ArrayList<>();

        HttpUrl nextUrl = HttpUrl.parse(restEndpointBaseUrl)
            .newBuilder()
            .addPathSegments("kafka/v3/clusters")
            .addPathSegment(clusterId)
            .addPathSegment("topics")
            .addQueryParameter("page_size", "1000")
            .build();

        while (nextUrl != null) {
            Request request = new Request.Builder()
                .url(nextUrl)
                .get()
                .addHeader("Authorization", basicAuthHeader)
                .addHeader("Accept", "application/json")
                .build();

            try (Response response = httpClient.newCall(request).execute()) {
                String body = response.body() != null ? response.body().string() : "";
                if (!response.isSuccessful()) {
                    int code = response.code();
                    if (code == 401 || code == 403) {
                        throw new IOException("Unauthorized to access Kafka REST API. Verify Kafka API key/secret and RBAC permissions. HTTP " + code);
                    }
                    throw new IOException("Failed to list topics (HTTP " + code + "): " + body);
                }

                JsonNode root = objectMapper.readTree(body);
                JsonNode data = root.get("data");
                if (data != null && data.isArray()) {
                    for (JsonNode node : data) {
                        JsonNode name = node.get("topic_name");
                        if (name != null) {
                            topics.add(name.asText());
                        }
                    }
                }

                // pagination: metadata.next.href
                JsonNode metadata = root.get("metadata");
                JsonNode next = metadata != null ? metadata.get("next") : null;
                JsonNode href = next != null ? next.get("href") : null;
                if (href != null && !href.isNull()) {
                    String hrefText = href.asText();
                    // href may be absolute; OkHttp can parse it directly
                    nextUrl = HttpUrl.parse(hrefText);
                } else {
                    nextUrl = null;
                }
            }
        }

        return topics;
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


