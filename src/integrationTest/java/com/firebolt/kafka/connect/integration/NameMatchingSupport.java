package com.firebolt.kafka.connect.integration;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.clients.FireboltClient;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Shared fixtures for the record-field ↔ table-column name-matching ITs ({@code INSERT … BY NAME}).
 * The table mixes every column flavor a record can omit: nullable without default, nullable with
 * default, NOT NULL with default, and a non-constant default.
 */
public final class NameMatchingSupport {

    public static final Supplier<String> TABLE_SCHEMA = () -> "CREATE TABLE \"%s\" ("
            + "\"id\" INTEGER NOT NULL, "
            + "\"name\" TEXT NULL, "
            + "\"opt\" TEXT NULL, "
            + "\"opt_def\" INTEGER NULL DEFAULT 7, "
            + "\"req_def\" TEXT NOT NULL DEFAULT 'rd', "
            + "\"ts_def\" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP)";

    private NameMatchingSupport() {
    }

    /** One table row, as {@code id -> [name, opt, opt_def, req_def, ts_def IS NOT NULL]}. */
    public static Map<Integer, List<Object>> readRows(FireboltClient client, String table) throws SQLException {
        Map<Integer, List<Object>> rows = new LinkedHashMap<>();
        try (ResultSet rs = client.executeQuery(String.format(
                "SELECT \"id\", \"name\", \"opt\", \"opt_def\", \"req_def\", \"ts_def\" IS NOT NULL AS has_ts "
                        + "FROM \"%s\" ORDER BY \"id\"", table))) {
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                row.add(rs.getString("name"));
                row.add(rs.getString("opt"));
                Object optDef = rs.getObject("opt_def");
                row.add(optDef == null ? null : ((Number) optDef).intValue());
                row.add(rs.getString("req_def"));
                row.add(rs.getBoolean("has_ts"));
                rows.put(rs.getInt("id"), row);
            }
        }
        return rows;
    }

    public static List<Object> row(String name, String opt, Integer optDef, String reqDef) {
        List<Object> row = new ArrayList<>();
        row.add(name);
        row.add(opt);
        row.add(optDef);
        row.add(reqDef);
        row.add(true); // ts_def is always populated (DEFAULT CURRENT_TIMESTAMP, never sent by a record)
        return row;
    }

    public static KafkaConsumer<byte[], byte[]> dlqConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "name-matching-dlq-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    /** Polls the DLQ until {@code expected} records arrived (or the timeout passes) and returns the count. */
    public static int drainDlq(KafkaConsumer<byte[], byte[]> consumer, int expected, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int count = 0;
        while (count < expected && System.currentTimeMillis() < deadline) {
            count += consumer.poll(Duration.ofSeconds(1)).count();
        }
        // one more short poll so an over-delivery (more than expected) is visible to the assertion
        count += consumer.poll(Duration.ofSeconds(2)).count();
        return count;
    }

    public static void awaitTaskFailed(OkHttpClient http, ObjectMapper mapper, String connectHost, String connector) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
                .until(() -> "FAILED".equals(taskState(http, mapper, connectHost, connector)));
    }

    /**
     * Pauses the connector and waits until its task is paused, so records produced next are delivered
     * together — in one poll batch — after {@link #resume}.
     */
    public static void pause(OkHttpClient http, ObjectMapper mapper, String connectHost, String connector) throws IOException {
        put(http, connectHost + "/connectors/" + connector + "/pause");
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
                .until(() -> "PAUSED".equals(taskState(http, mapper, connectHost, connector)));
    }

    public static void resume(OkHttpClient http, String connectHost, String connector) throws IOException {
        put(http, connectHost + "/connectors/" + connector + "/resume");
    }

    private static void put(OkHttpClient http, String url) throws IOException {
        Request request = new Request.Builder().url(url).put(okhttp3.RequestBody.create(new byte[0])).build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("PUT " + url + " failed: " + response.code());
            }
        }
    }

    private static String taskState(OkHttpClient http, ObjectMapper mapper, String connectHost, String connector) throws IOException {
        Request request = new Request.Builder().url(connectHost + "/connectors/" + connector + "/status").get().build();
        try (Response response = http.newCall(request).execute()) {
            JsonNode tasks = mapper.readTree(response.body().string()).path("tasks");
            return tasks.isArray() && tasks.size() > 0 ? tasks.get(0).path("state").asText() : null;
        }
    }
}
