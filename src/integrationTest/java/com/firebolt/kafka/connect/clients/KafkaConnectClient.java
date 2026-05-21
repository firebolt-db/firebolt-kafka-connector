package com.firebolt.kafka.connect.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import static org.awaitility.Awaitility.await;

@Slf4j
@RequiredArgsConstructor
public class KafkaConnectClient {

    private final String baseUrl; // e.g., http://localhost:8083
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public String getConnectorState(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/connectors/" + connectorName + "/status")
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to get connector status: " + response.code() + " - " + body);
            }
            String responseBody = response.body().string();
            Map<String, Object> status = objectMapper.readValue(responseBody, Map.class);
            Map<String, Object> connector = (Map<String, Object>) status.get("connector");
            return (String) connector.get("state");
        } catch (Exception e) {
            log.debug("Error getting connector status for {}: {}", connectorName, e.getMessage());
            throw e;
        }
    }

    public void pauseConnector(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/connectors/" + connectorName + "/pause")
                .put(RequestBody.create(new byte[0], MediaType.parse("application/json")))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to pause connector: " + response.code() + " - " + body);
            }
        }
    }

    public void resumeConnector(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/connectors/" + connectorName + "/resume")
                .put(RequestBody.create(new byte[0], MediaType.parse("application/json")))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to resume connector: " + response.code() + " - " + body);
            }
        }
    }

    public void restartConnector(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/connectors/" + connectorName + "/restart?includeTasks=true&onlyFailed=false")
                .post(RequestBody.create(new byte[0], MediaType.parse("application/json")))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to restart connector: " + response.code() + " - " + body);
            }
        }
    }

    public void waitForConnectorRunning(String connectorName, Duration timeout) {
        log.info("Waiting for connector '{}' to be running...", connectorName);

        await()
                .atMost(timeout)
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> {
                    try {
                        String state = getConnectorState(connectorName);
                        log.debug("Connector '{}' state: {}", connectorName, state);
                        return "RUNNING".equalsIgnoreCase(state);
                    } catch (Exception e) {
                        log.debug("Error checking connector status: {}", e.getMessage());
                        return false;
                    }
                });

        log.info("Connector '{}' is running!", connectorName);
    }

    public Map<String, Object> getConnectorConfig(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/connectors/" + connectorName + "/config")
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("Failed to get connector config: " + response.code() + " - " + body);
            }
            String responseBody = response.body().string();
            return objectMapper.readValue(responseBody, Map.class);
        }
    }

    public Map<String, Object> updateConnectorConfig(String connectorName, Map<String, Object> newConfig) throws IOException {
        String json = objectMapper.writeValueAsString(newConfig);
        Request request = new Request.Builder()
                .url(baseUrl + "/connectors/" + connectorName + "/config")
                .put(RequestBody.create(json, MediaType.parse("application/json")))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                throw new IOException("Failed to update connector config: " + response.code() + " - " + body);
            }
            return objectMapper.readValue(body, Map.class);
        }
    }
}


