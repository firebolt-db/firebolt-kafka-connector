package com.firebolt.kafka.connect.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Client for interacting with Confluent Schema Registry.
 * Provides methods for schema registration, deletion, and retrieval.
 */
@Slf4j
public class SchemaRegistryClient {
    
    private final String schemaRegistryUrl;
    private final ObjectMapper objectMapper;
    private final OkHttpClient httpClient;
    
    /**
     * Creates a new SchemaRegistryClient with the specified URL.
     * 
     * @param schemaRegistryUrl the base URL of the Schema Registry
     */
    public SchemaRegistryClient(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.objectMapper = new ObjectMapper();
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
    }
    
    /**
     * Creates a new SchemaRegistryClient with custom HTTP client and ObjectMapper.
     * 
     * @param schemaRegistryUrl the base URL of the Schema Registry
     * @param httpClient the HTTP client to use for requests
     * @param objectMapper the ObjectMapper for JSON processing
     */
    public SchemaRegistryClient(String schemaRegistryUrl, OkHttpClient httpClient, ObjectMapper objectMapper) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Registers a new schema in the Schema Registry with specific schema type.
     * 
     * @param subject the subject name for the schema
     * @param schema the schema definition as JSON string
     * @param schemaType the schema type (e.g., "AVRO", "JSON", "PROTOBUF")
     * @return the schema ID
     * @throws IOException if schema registration fails
     */
    public int registerSchema(String subject, String schema, String schemaType) throws IOException {
        log.info("Registering {} schema for subject: {}", schemaType, subject);
        
        Map<String, Object> schemaRequest = new HashMap<>();
        schemaRequest.put("schema", schema);
        schemaRequest.put("schemaType", schemaType);
        
        String json = objectMapper.writeValueAsString(schemaRequest);
        
        RequestBody body = RequestBody.create(json, MediaType.parse("application/json"));
        Request request = new Request.Builder()
                .url(schemaRegistryUrl + "/subjects/" + subject + "/versions")
                .post(body)
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body().string();
            
            if (response.isSuccessful()) {
                Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                int schemaId = (Integer) responseMap.get("id");
                log.info("Schema registered successfully for subject '{}' with ID: {}", subject, schemaId);
                return schemaId;
            } else {
                log.error("Failed to register schema for subject '{}'. Response: {}", subject, responseBody);
                throw new RuntimeException("Failed to register schema: " + responseBody);
            }
        }
    }
    
    /**
     * Deletes a specific schema subject from the Schema Registry.
     * 
     * @param subject the subject name to delete
     * @throws IOException if deletion fails
     */
    public void deleteSchema(String subject) throws IOException {
        log.info("Deleting schema subject: {}", subject);
        
        Request request = new Request.Builder()
                .url(schemaRegistryUrl + "/subjects/" + subject)
                .delete()
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                log.info("Schema subject '{}' deleted successfully", subject);
            } else {
                String responseBody = response.body().string();
                if (response.code() == 404) {
                    log.info("Schema subject '{}' not found (already deleted)", subject);
                } else {
                    log.error("Failed to delete schema subject '{}'. Response: {}", subject, responseBody);
                    throw new RuntimeException("Failed to delete schema: " + responseBody);
                }
            }
        }
    }
    
    /**
     * Deletes all schema subjects from the Schema Registry.
     * 
     * @throws IOException if deletion fails
     */
    public void deleteAllSchemas() throws IOException {
        log.info("Deleting all schema subjects");
        
        // First get all subjects
        String[] subjects = getAllSchemaSubjects();
        
        if (subjects.length == 0) {
            log.info("No schema subjects found to delete");
            return;
        }
        
        // Delete each subject
        for (String subject : subjects) {
            deleteSchema(subject);
        }
        
        log.info("All schema subjects deleted successfully");
    }
    
    /**
     * Gets all schema subjects from the Schema Registry.
     * 
     * @return array of subject names
     * @throws IOException if retrieval fails
     */
    public String[] getAllSchemaSubjects() throws IOException {
        log.debug("Retrieving all schema subjects");
        
        Request request = new Request.Builder()
                .url(schemaRegistryUrl + "/subjects")
                .get()
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body().string();
            
            if (response.isSuccessful()) {
                String[] subjects = objectMapper.readValue(responseBody, String[].class);
                log.debug("Found {} schema subjects", subjects.length);
                return subjects;
            } else {
                log.error("Failed to retrieve schema subjects. Response: {}", responseBody);
                throw new RuntimeException("Failed to retrieve schema subjects: " + responseBody);
            }
        }
    }
    
    /**
     * Gets the schema for a specific subject and version.
     * 
     * @param subject the subject name
     * @param version the version (e.g., "latest" or specific version number)
     * @return the schema as a JSON string
     * @throws IOException if retrieval fails
     */
    public String getSchema(String subject, String version) throws IOException {
        log.debug("Retrieving schema for subject '{}' version '{}'", subject, version);
        
        Request request = new Request.Builder()
                .url(schemaRegistryUrl + "/subjects/" + subject + "/versions/" + version)
                .get()
                .build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body().string();
            
            if (response.isSuccessful()) {
                Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                String schema = (String) responseMap.get("schema");
                log.debug("Retrieved schema for subject '{}' version '{}'", subject, version);
                return schema;
            } else {
                log.error("Failed to retrieve schema for subject '{}' version '{}'. Response: {}", 
                         subject, version, responseBody);
                throw new RuntimeException("Failed to retrieve schema: " + responseBody);
            }
        }
    }
    
    /**
     * Closes the HTTP client and releases resources.
     */
    public void close() {
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }
} 