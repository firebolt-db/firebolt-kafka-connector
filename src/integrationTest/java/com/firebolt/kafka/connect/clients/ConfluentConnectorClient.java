package com.firebolt.kafka.connect.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
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
 * Client for Confluent Cloud Connect REST API to manage connectors.
 */
@Slf4j
public class ConfluentConnectorClient implements AutoCloseable {

    private static final String DEFAULT_API_BASE = "https://api.confluent.cloud";

    private final String environmentId;
    private final String connectClusterId;
    private final String basicAuthHeader;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiBaseUrl;

    public ConfluentConnectorClient(String environmentId,
                                    String connectClusterId,
                                    String apiKey,
                                    String apiSecret) {
        this(environmentId, connectClusterId, apiKey, apiSecret, DEFAULT_API_BASE,
            new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build(),
            new ObjectMapper());
    }

    public ConfluentConnectorClient(String environmentId,
                                    String connectClusterId,
                                    String apiKey,
                                    String apiSecret,
                                    String apiBaseUrl,
                                    OkHttpClient httpClient,
                                    ObjectMapper objectMapper) {
        if (environmentId == null || environmentId.isEmpty()) {
            throw new IllegalArgumentException("environmentId must be provided");
        }
        if (connectClusterId == null || connectClusterId.isEmpty()) {
            throw new IllegalArgumentException("connectClusterId must be provided");
        }
        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            throw new IllegalArgumentException("Confluent Cloud API key and secret must be provided");
        }
        this.environmentId = environmentId;
        this.connectClusterId = connectClusterId;
        this.apiBaseUrl = apiBaseUrl == null || apiBaseUrl.isEmpty() ? DEFAULT_API_BASE : apiBaseUrl;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.basicAuthHeader = buildBasicAuthHeader(apiKey, apiSecret);
    }

    /**
     * Lists connectors for an explicit environment and Connect cluster ID (lcc-...).
     */
    public List<String> listConnectors(String envId, String connectClusterId) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId)
            .addPathSegment("connectors")
            .build();
        Request request = new Request.Builder().url(url).get().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("list connectors", response.code(), body);
            }
            List<String> names = new ArrayList<>();
            try {
                names = objectMapper.readValue(body, objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
            } catch (Exception e) {
                Map<String, Object> map = objectMapper.readValue(body, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
                names.addAll(map.keySet());
            }
            return names;
        }
    }

    /**
     * Lists connectors with detailed information including IDs (similar to 'confluent connect list -o json').
     * This returns the full connector objects with metadata including the connector ID.
     */
    public List<Map<String, Object>> listConnectorsDetailed() throws IOException {
        return listConnectorsDetailed(this.environmentId, this.connectClusterId);
    }

    /**
     * Lists connectors with detailed information including IDs for explicit env and Connect cluster IDs.
     * This returns the full connector objects with metadata including the connector ID.
     * Uses the Confluent Cloud accounts API: /api/accounts/{envId}/clusters/{clusterId}/connectors
     */
    public List<Map<String, Object>> listConnectorsDetailed(String envId, String connectClusterId) throws IOException {
        // Use the correct Confluent Cloud accounts API endpoint
        String accountsApiUrl = "https://confluent.cloud/api/accounts/" + envId + "/clusters/" + connectClusterId + "/connectors";
        
        HttpUrl url = HttpUrl.parse(accountsApiUrl)
            .newBuilder()
            .addQueryParameter("expand", "status,info,id")  // Request detailed info including IDs
            .build();
        Request request = new Request.Builder().url(url).get().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("list connectors detailed", response.code(), body);
            }
            
            log.debug("Connectors detailed API response: {}", body);
            
            // The API might return different formats, try to handle both array and object responses
            try {
                // Try to parse as an array of connector objects
                return objectMapper.readValue(body, objectMapper.getTypeFactory().constructCollectionType(List.class, 
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class)));
            } catch (Exception e) {
                // If that fails, try to parse as a map and extract values
                Map<String, Object> responseMap = objectMapper.readValue(body, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
                List<Map<String, Object>> connectors = new ArrayList<>();
                
                // Check if there's a "data" field (common in API responses)
                Object data = responseMap.get("data");
                if (data instanceof List) {
                    for (Object item : (List<?>) data) {
                        if (item instanceof Map) {
                            connectors.add((Map<String, Object>) item);
                        }
                    }
                    return connectors;
                }
                
                // Otherwise, treat each value in the map as a connector object
                for (Object value : responseMap.values()) {
                    if (value instanceof Map) {
                        connectors.add((Map<String, Object>) value);
                    }
                }
                return connectors;
            }
        }
    }

    public Map<String, Object> getConnectorConfig(String connectorName) throws IOException {
        HttpUrl url = baseUrl().addPathSegment("connectors").addPathSegment(connectorName).build();
        Request request = new Request.Builder().url(url).get().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("get connector config", response.code(), body);
            }
            return objectMapper.readValue(body, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
        }
    }

    public void updateConnectorConfig(String connectorName, Map<String, String> newConfig) throws IOException {
        String json = objectMapper.writeValueAsString(newConfig);
        RequestBody requestBody = RequestBody.create(json, MediaType.parse("application/json"));
        HttpUrl url = baseUrl().addPathSegment("connectors").addPathSegment(connectorName).addPathSegment("config").build();
        Request request = new Request.Builder().url(url).put(requestBody).addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("update connector config", response.code(), body);
            }
        }
    }

    /**
     * Updates a connector's configuration for explicit env and Connect cluster IDs.
     */
    public void updateConnectorConfig(String envId, String connectClusterId, String connectorName, Map<String, String> newConfig) throws IOException {
        String json = objectMapper.writeValueAsString(newConfig);
        RequestBody requestBody = RequestBody.create(json, MediaType.parse("application/json"));
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId)
            .addPathSegment("connectors")
            .addPathSegment(connectorName)
            .addPathSegment("config")
            .build();
        Request request = new Request.Builder().url(url).put(requestBody).addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("update connector config", response.code(), body);
            }
        }
    }

    public void pauseConnector(String connectorName) throws IOException {
        HttpUrl url = baseUrl().addPathSegment("connectors").addPathSegment(connectorName).addPathSegment("pause").build();
        Request request = new Request.Builder().url(url).put(RequestBody.create(new byte[0], null)).addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("pause connector", response.code(), body);
            }
        }
    }

    public void resumeConnector(String connectorName) throws IOException {
        HttpUrl url = baseUrl().addPathSegment("connectors").addPathSegment(connectorName).addPathSegment("resume").build();
        Request request = new Request.Builder().url(url).put(RequestBody.create(new byte[0], null)).addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("resume connector", response.code(), body);
            }
        }
    }

    /**
     * Pauses a connector for explicit env and Connect cluster IDs.
     */
    public void pauseConnector(String envId, String connectClusterId, String connectorName) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId)
            .addPathSegment("connectors")
            .addPathSegment(connectorName)
            .addPathSegment("pause")
            .build();
        Request request = new Request.Builder().url(url).put(RequestBody.create(new byte[0], null)).addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("pause connector", response.code(), body);
            }
        }
    }

    /**
     * Resumes a connector for explicit env and Connect cluster IDs.
     */
    public void resumeConnector(String envId, String connectClusterId, String connectorName) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId)
            .addPathSegment("connectors")
            .addPathSegment(connectorName)
            .addPathSegment("resume")
            .build();
        Request request = new Request.Builder().url(url).put(RequestBody.create(new byte[0], null)).addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("resume connector", response.code(), body);
            }
        }
    }

    /**
     * Deletes a connector using the instance's environment and connect cluster IDs.
     */
    public void deleteConnector(String connectorName) throws IOException {
        deleteConnector(this.environmentId, this.connectClusterId, connectorName);
    }

    /**
     * Deletes a connector for explicit env and Connect cluster IDs.
     */
    public void deleteConnector(String envId, String connectClusterId, String connectorName) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId)
            .addPathSegment("connectors")
            .addPathSegment(connectorName)
            .build();
        Request request = new Request.Builder().url(url).delete().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("delete connector", response.code(), body);
            }
        }
    }

    // Backwards-compat helper if needed internally; prefer listSinkConnectorPlugins
    private static String safeString(Object o) { return o == null ? null : o.toString(); }

    /**
     * Best-effort check if a custom plugin id (e.g., ccp-...) exists on the Connect cluster.
     * This scans the connector-plugins payload for a matching id in common fields.
     * Returns true if a matching id is found, false otherwise.
     */
    public boolean customPluginIdExists(String cloudName, String pluginId) throws IOException {
        // Prefer custom plugin registry for existence check
        List<Map<String, Object>> plugins = listCustomConnectorPluginsConnectApi(cloudName, 100);
        for (Map<String, Object> p : plugins) {
            // try a few likely keys that may carry an identifier
            Object id = p.get("id");
            if (id != null && pluginId.equals(id.toString())) return true;
            Object pid = p.get("plugin_id");
            if (pid != null && pluginId.equals(pid.toString())) return true;
            Object ccp = p.get("confluent_custom_plugin_id");
            if (ccp != null && pluginId.equals(ccp.toString())) return true;
        }
        return false;
    }

    /**
     * Lists custom connector plugins registered in the organization for a given environment (ccp-...).
     * Endpoint: GET /org/v2/custom-connector-plugins?environment=<env-id>
     * NOTE: Some orgs may not expose this endpoint; see listCustomConnectorPluginsConnectApi for an alternative.
     */
    public List<Map<String, Object>> listCustomConnectorPlugins(String envId) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("org/v2/custom-connector-plugins")
            .addQueryParameter("environment", envId)
            .build();

        Request request = new Request.Builder().url(url).get().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("list custom connector plugins", response.code(), body);
            }
            com.fasterxml.jackson.databind.JsonNode root = objectMapper.readTree(body);
            com.fasterxml.jackson.databind.JsonNode data = root.get("data");
            List<Map<String, Object>> result = new ArrayList<>();
            if (data != null && data.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode item : data) {
                    // Return each item as a Map<String,Object>
                    Map<String, Object> entry = objectMapper.convertValue(item, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
                    result.add(entry);
                }
            }
            return result;
        }
    }

    /**
     * Lists custom connector plugins via the Connect API variant.
     * Endpoint: GET /connect/v1/custom-connector-plugins?cloud=<AWS|GCP|AZURE>&page_size=N
     * Paginates using metadata.next.href when present.
     */
    public List<Map<String, Object>> listCustomConnectorPluginsConnectApi(String cloud, Integer pageSize) throws IOException {
        HttpUrl.Builder builder = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/custom-connector-plugins");
        if (cloud != null && !cloud.isEmpty()) {
            builder.addQueryParameter("cloud", cloud);
        }
        if (pageSize != null && pageSize > 0) {
            builder.addQueryParameter("page_size", String.valueOf(pageSize));
        }

        HttpUrl nextUrl = builder.build();
        List<Map<String, Object>> result = new ArrayList<>();

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
                    handleError("list custom connector plugins (connect api)", response.code(), body);
                }
                JsonNode root = objectMapper.readTree(body);
                JsonNode data = root.get("data");
                if (data != null && data.isArray()) {
                    for (JsonNode item : data) {
                        Map<String, Object> entry = objectMapper.convertValue(item,
                            objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
                        result.add(entry);
                    }
                }

                // pagination via metadata.next.href if present
                JsonNode metadata = root.get("metadata");
                JsonNode next = metadata != null ? metadata.get("next") : null;
                JsonNode href = next != null ? next.get("href") : null;
                if (href != null && !href.isNull()) {
                    nextUrl = HttpUrl.parse(href.asText());
                } else {
                    nextUrl = null;
                }
            }
        }

        return result;
    }
    /**
     * Creates a new connector in the specified environment and Connect cluster using a Confluent custom plugin id.
     * The provided config is merged into the connector config. Keys in providedConfig override defaults if present.
     * Required keys typically include at least: "connector.class", connector-specific settings, and topic mapping.
     */
    public Map<String, Object> createConnector(
            String envId,
            String connectClusterId,
            String connectorName,
            String pluginId,
            Map<String, String> providedConfig) throws IOException {
        return createConnector(envId, connectClusterId, connectorName, pluginId, null, providedConfig);
    }

    /**
     * Creates a new connector, allowing an explicit plugin type (e.g., "SINK" or "SOURCE").
     */
    public Map<String, Object> createConnector(
            String envId,
            String connectClusterId,
            String connectorName,
            String pluginId,
            String pluginType,
            Map<String, String> providedConfig) throws IOException {
        if (connectorName == null || connectorName.isEmpty()) {
            throw new IllegalArgumentException("connectorName must be provided");
        }
        if (pluginId == null || pluginId.isEmpty()) {
            throw new IllegalArgumentException("pluginId must be provided");
        }

        // Build connector body
        java.util.LinkedHashMap<String, Object> bodyRoot = new java.util.LinkedHashMap<>();
        bodyRoot.put("name", connectorName);

        java.util.LinkedHashMap<String, String> config = new java.util.LinkedHashMap<>();
        // Confluent custom plugin identifiers
        config.put("name", connectorName);
        config.put("confluent.connector.type", "CUSTOM");
        config.put("confluent.custom.plugin.id", pluginId);
        if (pluginType != null && !pluginType.isEmpty()) {
            config.put("confluent.custom.plugin.type", pluginType);
        }
        // Merge user-provided config
        if (providedConfig != null) {
            for (Map.Entry<String, String> e : providedConfig.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    config.put(e.getKey(), e.getValue());
                }
            }
        }

        bodyRoot.put("config", config);

        String json = objectMapper.writeValueAsString(bodyRoot);

        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId)
            .addPathSegment("connectors")
            .build();

        Request request = new Request.Builder()
            .url(url)
            .post(RequestBody.create(json, MediaType.parse("application/json")))
            .addHeader("Authorization", basicAuthHeader)
            .addHeader("Accept", "application/json")
            .addHeader("Content-Type", "application/json")
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("create connector", response.code(), body);
            }
            return objectMapper.readValue(body, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
        }
    }

    /**
     * Gets the connector ID by examining the connector's detailed information.
     * This makes an additional API call to describe the connector and extract the ID.
     * 
     * @param connectorName Name of the connector
     * @return The connector ID if found, null otherwise
     */
    public String getConnectorId(String connectorName) throws IOException {
        return getConnectorId(this.environmentId, this.connectClusterId, connectorName);
    }

    /**
     * Gets the connector ID by listing all connectors and finding the one with matching name.
     * This uses the same approach as 'confluent connect list -o json' to get connector IDs.
     * 
     * @param envId Environment ID
     * @param connectClusterId Connect cluster ID
     * @param connectorName Name of the connector
     * @return The connector ID if found, null otherwise
     */
    public String getConnectorId(String envId, String connectClusterId, String connectorName) {
        try {
            // List all connectors with detailed info (includes IDs)
            List<Map<String, Object>> connectors = listConnectorsDetailed(envId, connectClusterId);
            
            // Find the connector with matching name
            for (Map<String, Object> connector : connectors) {
                Object nameObj = ((Map<String, Object>) connector.get("info")).get("name");
                if (nameObj != null && connectorName.equals(nameObj.toString())) {
                    // Found the connector, extract its ID
                    String id = extractIdFromResponse(connector);
                    if (id != null) {
                        log.debug("Found connector ID '{}' for connector '{}'", id, connectorName);
                        return id;
                    }
                }
            }
            
            log.warn("Connector '{}' not found in connector list", connectorName);
            return null;
            
        } catch (Exception e) {
            log.warn("Failed to get connector ID for '{}': {}", connectorName, e.getMessage());
            return null;
        }
    }

    /**
     * Helper method to extract ID from various response types.
     * Looks for connector IDs, particularly those starting with 'lcc-' for Connect cluster connectors.
     */
    private String extractIdFromResponse(Map<String, Object> response) {
        if (response == null) {
            return null;
        }
        
        Object value = ((Map<String, String>) response.get("id")).get("id");
        if (value != null) {
          return value.toString();
        }

        return null;
    }

    public String getConnectorState(String connectorName) throws IOException {
        HttpUrl url = baseUrl().addPathSegment("connectors").addPathSegment(connectorName).addPathSegment("status").build();
        Request request = new Request.Builder().url(url).get().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("get connector status", response.code(), body);
            }
            Map<String, Object> status = objectMapper.readValue(body, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
            Object connector = status.get("connector");
            if (connector instanceof Map) {
                Object state = ((Map<?, ?>) connector).get("state");
                return state != null ? state.toString() : null;
            }
            return null;
        }
    }

    /**
     * Returns the full status payload for a connector for explicit env and cluster IDs.
     */
    public Map<String, Object> getConnectorStatus(String envId, String clusterId, String connectorName) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(envId)
            .addPathSegment("clusters")
            .addPathSegment(clusterId)
            .addPathSegment("connectors")
            .addPathSegment(connectorName)
            .addPathSegment("status")
            .build();
        Request request = new Request.Builder().url(url).get().addHeader("Authorization", basicAuthHeader).build();
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                handleError("get connector status", response.code(), body);
            }
            return objectMapper.readValue(body, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
        }
    }

    private HttpUrl.Builder baseUrl() {
        return HttpUrl.parse(apiBaseUrl)
            .newBuilder()
            .addPathSegments("connect/v1/environments")
            .addPathSegment(environmentId)
            .addPathSegment("clusters")
            .addPathSegment(connectClusterId);
    }

    private static String buildBasicAuthHeader(String apiKey, String apiSecret) {
        String credentials = apiKey + ":" + apiSecret;
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    private static void handleError(String action, int code, String body) throws IOException {
        if (code == 401 || code == 403) {
            throw new IOException("Unauthorized to " + action + ". HTTP " + code + ". Body: " + body);
        }
        throw new IOException("Failed to " + action + " (HTTP " + code + "): " + body);
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


