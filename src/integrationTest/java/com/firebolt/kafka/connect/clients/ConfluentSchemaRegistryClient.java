package com.firebolt.kafka.connect.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Confluent Cloud Schema Registry client with Basic authentication.
 * Supports listing subjects, registering schemas, and deleting subjects.
 */
@Slf4j
public class ConfluentSchemaRegistryClient implements AutoCloseable {

    private final String baseUrl; // e.g. https://psrc-xxxxx.region.provider.confluent.cloud
    private final String authHeader; // Basic <base64(apiKey:apiSecret)>
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public ConfluentSchemaRegistryClient(String baseUrl, String apiKey, String apiSecret) {
        this(baseUrl, apiKey, apiSecret,
            new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build(),
            new ObjectMapper());
    }

    public ConfluentSchemaRegistryClient(String baseUrl,
                                         String apiKey,
                                         String apiSecret,
                                         OkHttpClient httpClient,
                                         ObjectMapper objectMapper) {
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalArgumentException("baseUrl must be provided");
        }
        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            throw new IllegalArgumentException("Schema Registry API key and secret must be provided");
        }
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.authHeader = buildBasicAuthHeader(apiKey, apiSecret);
    }

    /**
     * Lists all schema subjects.
     */
    public String[] listSubjects() throws IOException {
        Request request = new Request.Builder()
            .url(baseUrl + "/subjects")
            .get()
            .addHeader("Authorization", authHeader)
            .addHeader("Accept", "application/vnd.schemaregistry.v1+json, application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (response.isSuccessful()) {
                return objectMapper.readValue(body, String[].class);
            }
            int code = response.code();
            if (code == 401 || code == 403) {
                throw new IOException("Unauthorized to list subjects. Verify SR API key/secret and permissions. HTTP " + code);
            }
            throw new IOException("Failed to list subjects (HTTP " + code + "): " + body);
        }
    }

    /**
     * Registers a new schema for a subject. schemaType may be AVRO (default), JSON, or PROTOBUF.
     * Returns the schema ID.
     */
    public int registerSchema(String subject, String schema, String schemaType) throws IOException {
        if (subject == null || subject.isEmpty()) {
            throw new IllegalArgumentException("subject must be provided");
        }
        if (schema == null || schema.isEmpty()) {
            throw new IllegalArgumentException("schema must be provided");
        }

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("schema", schema);
        if (schemaType != null && !schemaType.isEmpty()) {
            payload.put("schemaType", schemaType);
        }
        String json = objectMapper.writeValueAsString(payload);

        RequestBody body = RequestBody.create(json, MediaType.parse("application/json"));
        Request request = new Request.Builder()
            .url(baseUrl + "/subjects/" + subject + "/versions")
            .post(body)
            .addHeader("Authorization", authHeader)
            .addHeader("Accept", "application/vnd.schemaregistry.v1+json, application/json")
            .addHeader("Content-Type", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String respBody = response.body() != null ? response.body().string() : "";
            if (response.isSuccessful()) {
                return objectMapper.readTree(respBody).get("id").asInt();
            }
            int code = response.code();
            if (code == 401 || code == 403) {
                throw new IOException("Unauthorized to register schema. Verify SR API key/secret and permissions. HTTP " + code);
            }
            throw new IOException("Failed to register schema (HTTP " + code + "): " + respBody);
        }
    }

    /**
     * Deletes a subject. If permanent is true, uses the permanent delete (hard delete) query param.
     */
    public void deleteSubject(String subject, boolean permanent) throws IOException {
        if (subject == null || subject.isEmpty()) {
            throw new IllegalArgumentException("subject must be provided");
        }
        String url = baseUrl + "/subjects/" + subject;
        if (permanent) {
            url += "?permanent=true";
        }
        Request request = new Request.Builder()
            .url(url)
            .delete()
            .addHeader("Authorization", authHeader)
            .addHeader("Accept", "application/vnd.schemaregistry.v1+json, application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String respBody = response.body() != null ? response.body().string() : "";
            if (response.isSuccessful()) {
                return;
            }
            int code = response.code();
            if (code == 404) {
                throw new IOException("Subject not found: " + subject + " (HTTP 404)");
            }
            if (code == 401 || code == 403) {
                throw new IOException("Unauthorized to delete subject. Verify SR API key/secret and permissions. HTTP " + code);
            }
            throw new IOException("Failed to delete subject (HTTP " + code + "): " + respBody);
        }
    }

    public void deleteSubject(String subject) throws IOException {
        deleteSubject(subject, false);
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


