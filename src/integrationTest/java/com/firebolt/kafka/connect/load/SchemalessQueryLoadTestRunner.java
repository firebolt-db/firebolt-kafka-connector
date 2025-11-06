package com.firebolt.kafka.connect.load;

import com.firebolt.kafka.connect.clients.ConfluentConnectorClient;
import com.firebolt.kafka.connect.clients.ConfluentKafkaClient;
import com.firebolt.kafka.connect.clients.ConfluentResourceClient;
import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.utils.JdbcConnectionParser;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.awaitility.Awaitility.await;

/**
 * Runner for schemaless load test that fetches records from a query.
 * This is a temporary test runner that copies code from LoadTestRunner.
 */
@Slf4j
public class SchemalessQueryLoadTestRunner {

    private TestScenario testScenario;
    private String recordFetchQuery;
    private String postProcessingScript;

    public SchemalessQueryLoadTestRunner(TestScenario testScenario, String recordFetchQuery, String postProcessingScript) {
        this.testScenario = testScenario;
        this.recordFetchQuery = recordFetchQuery;
        this.postProcessingScript = postProcessingScript;
    }

    public LoadTestRunResult run() throws IOException, SQLException {
        long runStartTime = Instant.now().toEpochMilli();

        String cloudResourcesApiKey = testScenario.getConfluentCloudSettings().getCloudResourceApiKey();
        String cloudResourcesApiSecret = testScenario.getConfluentCloudSettings().getCloudResourceApiSecret();
        String environmentId = testScenario.getConfluentCloudSettings().getEnvironmentId();
        String connectorName = testScenario.getConnectorName();

        String kafkaApiKey = testScenario.getConfluentCloudSettings().getKafkaApiKey();
        String kafkaApiSecret = testScenario.getConfluentCloudSettings().getKafkaApiSecret();
        String clusterId = testScenario.getConfluentCloudSettings().getClusterId();

        String fireboltPluginId = testScenario.getConfluentCloudSettings().getFireboltConnectorPluginId();

        String topicName = testScenario.getTopicName();
        String tableName = topicName;

        String fireboltClientId = testScenario.getFireboltSettings().getClientId();
        String fireboltClientSecret = testScenario.getFireboltSettings().getClientSecret();
        String jdbcUrl = testScenario.getFireboltSettings().getJdbcUrl();

        // initialize clients with try-with-resources to ensure proper cleanup
        try (ConfluentResourceClient confluentResourceClient = new ConfluentResourceClient(cloudResourcesApiKey, cloudResourcesApiSecret)) {
            String clusterEndpointUrl = confluentResourceClient.getClusterEndpointUrl(clusterId, environmentId);
            String bootstrapServers = confluentResourceClient.getBootstrapServerUrl(clusterId, environmentId);

            try (ConfluentConnectorClient confluentConnectorClient = new ConfluentConnectorClient(environmentId, clusterId, cloudResourcesApiKey, cloudResourcesApiSecret);
                 ConfluentKafkaClient confluentKafkaClient = new ConfluentKafkaClient(clusterEndpointUrl, clusterId, kafkaApiKey, kafkaApiSecret);
                 FireboltClient fireboltClient = getFireboltClient(jdbcUrl, fireboltClientId, fireboltClientSecret)) {

                // make sure plugin exists
                if (!confluentConnectorClient.customPluginIdExists(testScenario.getConfluentCloudSettings().getCloudName(), fireboltPluginId)) {
                    throw new RuntimeException("Did not find the custom plugin " + fireboltPluginId);
                }

                // topic and table has to exist before the connector is created
                setupKafkaTopic(confluentKafkaClient, topicName);

                // Create Firebolt table if schema file is provided
                if (testScenario.getTableSchemaDefinitionFilePath() != null && !testScenario.getTableSchemaDefinitionFilePath().isEmpty()) {
                    createFireboltTable(fireboltClient, tableName);
                } else {
                    log.info("No table schema file provided, assuming table '{}' already exists", tableName);
                }

                // create a new connector from the plugin - schemaless configuration
                Map<String, String> connectorConfig = createSchemalessConnectorConfiguration();

                // override any specific attributes of the connector definition
                if (testScenario.getConnectorConfiguration() != null && !testScenario.getConnectorConfiguration().isEmpty()) {
                    connectorConfig.putAll(testScenario.getConnectorConfiguration());
                }

                // add the dynamic APIs as the firebolt account engine url
                Set<String> hostnames = new HashSet<>(testScenario.getStaticOutboundHostnames());
                hostnames.add(fireboltClient.getEngineUrl());
                List<String> networkEndpoints = createConnectorNetworkEndpoints(hostnames);
                log.info("Found : {} network endpoints", networkEndpoints);
                connectorConfig.put("confluent.custom.connection.endpoints", String.join(";", networkEndpoints));

                // create the connector
                Map<String, Object> createdConnectorConfig = confluentConnectorClient.createConnector(environmentId, clusterId, connectorName, fireboltPluginId, connectorConfig);
                createdConnectorConfig.entrySet().stream().forEach(entry -> log.info("Key: {}, [value] class: {},value  {} ", entry.getKey(), entry.getValue().getClass(), entry.getValue()));

                // wait for connector to be started successfully
                log.info("Waiting for connector to start");
                waitForConnectorToStart(confluentConnectorClient, connectorName);
                log.info("Connector {} is successfully running.", connectorName);

                // keep the connector id
                String connectorId = confluentConnectorClient.getConnectorId(connectorName);
                log.info("Connector id : {}", connectorId);

                // pause the connector (we will fetch and publish messages first, then start the connector)
                pauseConnector(confluentConnectorClient, connectorName);

                // Fetch records from query and publish to Kafka 10 times
                publishMessagesFromQuery(fireboltClient, recordFetchQuery, topicName, bootstrapServers, kafkaApiKey, kafkaApiSecret, 10);

                // once all messages have been published start the connector
                startConnector(confluentConnectorClient, connectorName);

                // wait until all messages are ingested into firebolt
                waitForDataInFirebolt(fireboltClient, tableName);

                // collect some statistics from the run
                LoadTestRunResult loadTestRunResult = collectAndPrintRunStats(fireboltClient, tableName, runStartTime);

                // stop connector
                pauseConnector(confluentConnectorClient, connectorName);

                // delete kafka topic
                confluentKafkaClient.deleteTopic(topicName);

                if (testScenario.isDeleteConnector()) {
                    log.info("Deleting the connector {}", connectorName);
                    confluentConnectorClient.deleteConnector(testScenario.getConnectorName());
                    log.info("Successfully deleted connector {}", testScenario.getConnectorName());

                    // the connector creates a topic in the format: <connectorName>-app-logs so delete the topic as well
                    String connectorLogsTopicName = connectorId + "-app-logs";
                    log.info("Delete {} connector log topic", connectorLogsTopicName);
                    confluentKafkaClient.deleteTopic(connectorLogsTopicName);
                }

                if (testScenario.isDeleteTable()) {
                    log.info("Dropping the table {}", tableName);
                    fireboltClient.dropTable(tableName);
                }

                return loadTestRunResult;
            }
        }
    }

    /**
     * Creates schemaless connector configuration.
     */
    private Map<String, String> createSchemalessConnectorConfiguration() {
        String topicName = testScenario.getTopicName();
        String tableName = topicName;
        String jdbcUrl = testScenario.getFireboltSettings().getJdbcUrl();
        String fireboltClientId = testScenario.getFireboltSettings().getClientId();
        String fireboltClientSecret = testScenario.getFireboltSettings().getClientSecret();
        String kafkaApiKey = testScenario.getConfluentCloudSettings().getKafkaApiKey();
        String kafkaApiSecret = testScenario.getConfluentCloudSettings().getKafkaApiSecret();

        Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("topic.to.table.mapping", topicName + ":" + tableName);
        connectorConfig.put("jdbc.connection.url", jdbcUrl);
        connectorConfig.put("poll.interval.ms", "1000");
        // Set max.poll.records to a high value to ensure 5MB limit is the primary constraint
        // If average record is ~1KB, 5MB = ~5000 records, so setting to 10000 ensures size limit is hit first
        connectorConfig.put("consumer.override.max.poll.records", "10000");
        // Set fetch.max.bytes to 5MB (5 * 1024 * 1024 = 5242880 bytes)
        // This is the primary constraint - consumer will fetch up to 5MB regardless of record count
        connectorConfig.put("fetch.max.bytes", "3000000");
        connectorConfig.put("consumer.override.fetch.max.bytes", "3000000");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        connectorConfig.put("schemas.enable", "false");
        // Set max.partition.fetch.bytes to 5MB to be consistent with fetch.max.bytes
        connectorConfig.put("max.partition.fetch.bytes", "3000000");
        connectorConfig.put("producer.override.max.request.size", "10485760");
        connectorConfig.put("consumer.override.max.partition.fetch.bytes", "3000000");
        connectorConfig.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");
        connectorConfig.put("tasks.max", "1");
        connectorConfig.put("kafka.api.key", kafkaApiKey);
        connectorConfig.put("kafka.api.secret", kafkaApiSecret);
        connectorConfig.put("firebolt.clientId", fireboltClientId);
        connectorConfig.put("firebolt.clientSecret", fireboltClientSecret);
        connectorConfig.put("errors.tolerance", "all");
        connectorConfig.put("errors.deadletterqueue.topic.name", "dlq-topic-firebolt");
        connectorConfig.put("errors.deadletterqueue.context.headers.enable", "true");

        // Add post-processing script if provided
        if (postProcessingScript != null && !postProcessingScript.isEmpty()) {
            connectorConfig.put("post.processing.script", postProcessingScript);
        }

        return connectorConfig;
    }

    private LoadTestRunResult collectAndPrintRunStats(FireboltClient fireboltClient, String tableName, long runStartTime) throws SQLException {
        long runEndTime = Instant.now().toEpochMilli();

        // wait for about 30 seconds for the query history to have the latest data
        sleepForMillis(TimeUnit.SECONDS.toMillis(30));

        // find how much it took for the first record to be inserted and last one
        Pair<Instant, Instant> minMaxInstants = findDurationInSecondBetweenFistInsertedRowAndLastInsertedRow(fireboltClient, tableName, runStartTime, runEndTime);
        long fireboltIngestionDurationInSeconds = minMaxInstants.getRight().getEpochSecond() - minMaxInstants.getLeft().getEpochSecond();
        if (fireboltIngestionDurationInSeconds == 0) {
            fireboltIngestionDurationInSeconds = 1;
        }

        List<String> queryHistoryResults = getQueryHistoryResults(fireboltClient, tableName, runStartTime, runEndTime);
        
        // For query-based tests, we don't know the exact message count, so use row count
        int messageCount = fireboltClient.countRows(tableName);

        return LoadTestRunResult.builder()
                .fireboltIngestionRate(messageCount / fireboltIngestionDurationInSeconds)
                .fireboltTotalIngestionDuration(Duration.ofSeconds(fireboltIngestionDurationInSeconds))
                .queryHistoryDetails(queryHistoryResults)
                .build();
    }

    private List<String> getQueryHistoryResults(FireboltClient fireboltClient, String tableName, long runStartTimeInMillis, long runEndTimeInMillis) throws SQLException {
        String sql = "select query_id,submitted_time,start_time, end_time, duration_us, inserted_rows, inserted_bytes from information_schema.engine_query_history \n" +
                "  where query_text like 'INSERT INTO \"%s\" %%' \n" +
                "  and status = 'ENDED_SUCCESSFULLY'\n" +
                "  and start_time >= FROM_UNIXTIME(%d) " +
                "  and end_time <= FROM_UNIXTIME(%d)" +
                "  order by start_time desc;";

        ResultSet resultSet = fireboltClient.executeQuery(String.format(sql, tableName, runStartTimeInMillis/1000, runEndTimeInMillis/1000));
        List<String> queryHistoryDetails = new ArrayList<>();

        while(resultSet.next()) {
            queryHistoryDetails.add(
                new StringBuilder()
                        .append("[queryId: ").append(resultSet.getString(1)).append("], ")
                        .append("[submittedTime: ").append(resultSet.getString(2)).append("], ")
                        .append("[startTime: ").append(resultSet.getString(3)).append("], ")
                        .append("[endTime: ").append(resultSet.getString(4)).append("], ")
                        .append("[durationMicros: ").append(resultSet.getString(5)).append("], ")
                        .append("[insertedRows: ").append(resultSet.getString(6)).append("], ")
                        .append("[insertedBytes: ").append(resultSet.getString(7)).append("] ")
                        .toString());
        }

        return queryHistoryDetails;
    }

    private void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("Thread was interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private Pair<Instant, Instant> findDurationInSecondBetweenFistInsertedRowAndLastInsertedRow(FireboltClient fireboltClient, String tableName, long afterDateInMillis, long beforeDateInMillis) throws SQLException {
        String sql = "select min(start_time), max(end_time) from information_schema.engine_query_history \n" +
                "  where query_text like 'INSERT INTO \"%s\" %%' \n" +
                "  and status = 'ENDED_SUCCESSFULLY'\n" +
                "  and start_time >= FROM_UNIXTIME(%d) " +
                "  and end_time <= FROM_UNIXTIME(%d);";

        ResultSet resultSet = fireboltClient.executeQuery(String.format(sql, tableName, afterDateInMillis/1000, beforeDateInMillis/1000));
        resultSet.next();
        Instant min = resultSet.getTimestamp(1).toInstant();
        Instant max = resultSet.getTimestamp(2).toInstant();
        return Pair.of(min, max);
    }

    private void waitForConnectorToStart(ConfluentConnectorClient confluentConnectorClient, String connectorName) {
        waitForState(confluentConnectorClient, connectorName, "running", Duration.ofMinutes(5));
    }

    private void waitForDataInFirebolt(FireboltClient fireboltClient, String tableName) {
        // For query-based, wait for at least 1 record, then wait additional time for all records
        log.info("Waiting for data in Firebolt table '{}' (query-based, count unknown)...", tableName);
        await()
                .atMost(testScenario.getFireboltIngestionWaitDuration())
                .pollInterval(Duration.ofSeconds(5))
                .until(() -> {
                    try {
                        int count = fireboltClient.countRows(tableName);
                        log.debug("Current row count in table '{}': {}", tableName, count);
                        return count == 25000;
                    } catch (SQLException e) {
                        log.debug("Error querying Firebolt table: {}", e.getMessage());
                        return false;
                    }
                });
        // Wait additional time for all records to be ingested
        log.info("Initial data found, waiting additional time for all records to be ingested...");
        sleepForMillis(TimeUnit.SECONDS.toMillis(30));
        log.info("Found expected data in Firebolt table '{}'", tableName);
    }

    /**
     * Fetches records from Firebolt using the provided query and publishes them to Kafka as JSON messages.
     * @param repeatCount number of times to publish the same records (default: 1)
     */
    private static void publishMessagesFromQuery(FireboltClient fireboltClient, String query, String topicName,
                                                 String bootstrapServers, String kafkaApiKey, String kafkaApiSecret,
                                                 int repeatCount) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        try (Producer<String, String> producer = initializeSchemalessProducer(bootstrapServers, kafkaApiKey, kafkaApiSecret)) {
            log.info("Executing query to fetch records: {}", query);
            ResultSet resultSet = fireboltClient.executeQuery(query);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            long start = System.currentTimeMillis();

            // Collect all records first
            List<Map<String, Object>> records = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> record = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = resultSet.getObject(i);
                    record.put(columnName, value);
                }
                records.add(record);
            }
            resultSet.close();
            
            int recordsPerIteration = records.size();
            int totalMessages = recordsPerIteration * repeatCount;
            final CountDownLatch latch = new CountDownLatch(totalMessages);
            
            log.info("Found {} records to publish, repeating {} times (total: {} messages)", recordsPerIteration, repeatCount, totalMessages);
            
            // Publish records repeatCount times
            for (int iteration = 1; iteration <= repeatCount; iteration++) {
                log.info("Publishing iteration {}/{}", iteration, repeatCount);
                
                for (Map<String, Object> record : records) {
                    // Convert to JSON string
                    String jsonValue = objectMapper.writeValueAsString(record);
                    
                    // Use first column as key, or generate a key with iteration suffix to make keys unique
                    String key = record.values().iterator().next() != null 
                            ? record.values().iterator().next().toString() + "_iter" + iteration
                            : String.valueOf(System.currentTimeMillis()) + "_iter" + iteration;
                    
                    ProducerRecord<String, String> pr = new ProducerRecord<>(topicName, key, jsonValue);
                    producer.send(pr, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                log.error("Produce failed: {}", exception.getMessage());
                            }
                            latch.countDown();
                        }
                    });
                }
            }
            
            // Flush and wait
            producer.flush();
            boolean completed = latch.await(Math.max(30L, totalMessages / 100), TimeUnit.SECONDS);
            long tookMs = System.currentTimeMillis() - start;
            log.info("Published {} messages ({} records x {} iterations) from query. Completed: {}. Elapsed: {} ms", 
                    totalMessages, recordsPerIteration, repeatCount, completed, tookMs);
            
        } catch (Exception e) {
            log.error("Failed to fetch and publish messages from query", e);
            throw new RuntimeException("Failed to publish messages from query", e);
        }
    }

    private static Producer<String, String> initializeSchemalessProducer(String bootstrapServers, 
                                                                          String kafkaApiKey, 
                                                                          String kafkaApiSecret) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64_000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Confluent Cloud Kafka auth (SASL_SSL)
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + kafkaApiKey + "' password='" + kafkaApiSecret + "';");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", 45000);

        return new KafkaProducer<>(props);
    }

    private static void setupKafkaTopic(ConfluentKafkaClient client, String topicName) throws IOException {
        List<String> topics = client.listTopics();
        log.info("Found {} topics", topics.size());

        topics.stream().forEach(topic -> log.debug("\nTopic : {}" + topic));

        if (topics.contains(topicName)) {
            log.warn("Topic {} already exists", topicName);
            return;
        }

        // create the topic
        log.debug("Creating the topic: {}", topicName);
        client.createTopic(topicName, 1, (short) 3, /*retentionMs*/ 604800000L, /*retentionBytes*/ null);

        log.info("Topic {} created", topicName);
    }

    private FireboltClient getFireboltClient(String jdbcUrl, String clientId, String clientSecret) throws SQLException {
        // Provide credentials and JDBC URL to FireboltClient via system properties
        System.setProperty("clientId", clientId);
        System.setProperty("clientSecret", clientSecret);
        System.setProperty("jdbc.connection.url", jdbcUrl);

        String database = JdbcConnectionParser.getDatabase(jdbcUrl);
        return FireboltClient.createFor(database);
    }

    private void createFireboltTable(FireboltClient client, String tableName) {
        try {
            String tableSchema = new String(java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get(testScenario.getTableSchemaDefinitionFilePath())));
            client.createTable(tableName, tableSchema);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Firebolt table", e);
        }
    }

    private void startConnector(ConfluentConnectorClient client, String connectorName) throws IOException {
        String state = client.getConnectorState(connectorName);
        log.info("Found connector {} in state {}", connectorName, state);

        if ("paused".equalsIgnoreCase(state)) {
            log.info("Resuming the connector {}", connectorName);
            client.resumeConnector(connectorName);
            waitForState(client, connectorName, "running");
        }

        log.info("The connector {} is in {} state", connectorName, state);
    }

    private static void pauseConnector(ConfluentConnectorClient client, String connectorName) throws IOException {
        String state = client.getConnectorState(connectorName);
        log.info("Found connector {} in state {}", connectorName, state);

        if ("running".equalsIgnoreCase(state)) {
            log.info("Pausing the connector {}", connectorName);
            client.pauseConnector(connectorName);
            waitForState(client, connectorName, "paused");
        }

        log.info("The connector {} is in {} state", connectorName, state);
    }

    private static void waitForState(ConfluentConnectorClient client, String connectorName, String expectedState, Duration duration) {
        long deadline = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                String current = client.getConnectorState(connectorName);
                if (expectedState.equalsIgnoreCase(current)) {
                    log.info("Connector {} is now in state {}", connectorName, expectedState);
                    break;
                }
                Thread.sleep(10000L);
            } catch (Exception e) {
                log.debug("Waiting for connector state failed once: {}", e.getMessage());
                try { Thread.sleep(10000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static void waitForState(ConfluentConnectorClient client, String connectorName, String expectedState) {
        waitForState(client, connectorName, expectedState, Duration.ofMinutes(1));
    }

    private static List<String> createConnectorNetworkEndpoints(Set<String> endpoints) {
        java.util.LinkedHashSet<String> results = new java.util.LinkedHashSet<>();

        if (endpoints == null || endpoints.isEmpty()) {
            return new java.util.ArrayList<>(results);
        }

        for (String raw : endpoints) {
            if (raw == null) {
                continue;
            }
            String host = raw.trim();
            if (host.isEmpty()) {
                continue;
            }
            // Strip scheme if present
            int schemeIdx = host.indexOf("://");
            if (schemeIdx >= 0) {
                host = host.substring(schemeIdx + 3);
            }
            // Cut path/query if present
            int slashIdx = host.indexOf('/') ;
            if (slashIdx >= 0) {
                host = host.substring(0, slashIdx);
            }
            // Cut port if present
            int colonIdx = host.indexOf(':');
            if (colonIdx >= 0) {
                host = host.substring(0, colonIdx);
            }

            if (host.isEmpty()) {
                continue;
            }

            // Add hostname
            results.add(host + ":443:TCP");

            // Resolve to IPs and add each IP
            try {
                java.net.InetAddress[] addresses = java.net.InetAddress.getAllByName(host);
                for (java.net.InetAddress addr : addresses) {
                    // Keep only IPv4 addresses
                    if (addr instanceof java.net.Inet4Address) {
                        String ip = addr.getHostAddress();
                        if (ip != null && !ip.isEmpty()) {
                            results.add(ip + ":443:TCP");
                        }
                    }
                }
            } catch (java.net.UnknownHostException e) {
                log.warn("Failed to resolve host '{}': {}", host, e.getMessage());
            }
        }

        return new java.util.ArrayList<>(results);
    }
}

