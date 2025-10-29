package com.firebolt.kafka.connect.load;

import com.firebolt.kafka.connect.clients.ConfluentConnectorClient;
import com.firebolt.kafka.connect.clients.ConfluentKafkaClient;
import com.firebolt.kafka.connect.clients.ConfluentResourceClient;
import com.firebolt.kafka.connect.clients.ConfluentSchemaRegistryClient;
import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.utils.JdbcConnectionParser;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Runs one load test:
 *  - creates the Kafka topic(s)
 *  - creates the Firebolt tables
 *  - creates and configure Firebolt Sink Connector
 *  - publishes the messages
 *  - verifies that the records were synced to Firebolt
 *  - computes some statistics
 */
@Slf4j
public class LoadTestRunner {

    private TestScenario testScenario;

    private static final String KAFKA_METADATA_TABLE = "KafkaSinkConnectorMetadata";

    public LoadTestRunner(TestScenario testScenario) {
        this.testScenario = testScenario;
    }

    public LoadTestRunResult run() throws IOException, SQLException {
        long runStartTime = Instant.now().toEpochMilli();

        log.info("Using the message size of roughly {} bytes", testScenario.getAverageMessageSizeInBytes());
        TestRecordFactory testRecordFactory = new TestRecordFactory(testScenario.getAverageMessageSizeInBytes());

        String cloudResourcesApiKey = testScenario.getConfluentCloudSettings().getCloudResourceApiKey();
        String cloudResourcesApiSecret = testScenario.getConfluentCloudSettings().getCloudResourceApiSecret();
        String environmentId = testScenario.getConfluentCloudSettings().getEnvironmentId();
        String connectorName = testScenario.getConnectorName();

        String kafkaApiKey = testScenario.getConfluentCloudSettings().getKafkaApiKey();
        String kafkaApiSecret = testScenario.getConfluentCloudSettings().getKafkaApiSecret();
        String clusterId = testScenario.getConfluentCloudSettings().getClusterId();

        String schemaApiKey = testScenario.getConfluentCloudSettings().getSchemaRegistryApiKey();
        String schemaApiSecret = testScenario.getConfluentCloudSettings().getSchemaRegistryApiSecret();

        String fireboltPluginId = testScenario.getConfluentCloudSettings().getFireboltConnectorPluginId();

        String topicName = testScenario.getTopicName();
        String tableName = topicName;

        String fireboltClientId = testScenario.getFireboltSettings().getClientId();
        String fireboltClientSecret = testScenario.getFireboltSettings().getClientSecret();
        String jdbcUrl = testScenario.getFireboltSettings().getJdbcUrl();

        // initialize clients with try-with-resources to ensure proper cleanup
        try (ConfluentResourceClient confluentResourceClient = new ConfluentResourceClient(cloudResourcesApiKey, cloudResourcesApiSecret)) {
            String schemaRegistryUrl = confluentResourceClient.getSchemaRegistryUrl(environmentId);
            String clusterEndpointUrl = confluentResourceClient.getClusterEndpointUrl(clusterId, environmentId);
            String bootstrapServers = confluentResourceClient.getBootstrapServerUrl(clusterId, environmentId);

            try (ConfluentConnectorClient confluentConnectorClient = new ConfluentConnectorClient(environmentId, clusterId, cloudResourcesApiKey, cloudResourcesApiSecret);
                 ConfluentKafkaClient confluentKafkaClient = new ConfluentKafkaClient(clusterEndpointUrl, clusterId, kafkaApiKey, kafkaApiSecret);
                 ConfluentSchemaRegistryClient schemaRegistryClient = new ConfluentSchemaRegistryClient(schemaRegistryUrl, schemaApiKey, schemaApiSecret);
                 FireboltClient fireboltClient = getFireboltClient(jdbcUrl, fireboltClientId, fireboltClientSecret)) {

                // make sure plugin exists
                if (!confluentConnectorClient.customPluginIdExists(testScenario.getConfluentCloudSettings().getCloudName(), fireboltPluginId)) {
                    throw new RuntimeException("Did not find the custom plugin " + fireboltPluginId);
                }

                // topic and table has to exist before the connector is created
                setupKafkaTopic(confluentKafkaClient, topicName);

                // TODO make sure the engine is running
                createFireboltTable(fireboltClient, tableName);

                // create a new connector from the plugin
                Map<String, String> connectorConfig = createDefaultConnectorConfiguration(schemaRegistryUrl);

                // override any specific attributes of the connector definition
                if (testScenario.getConnectorConfiguration() != null && !testScenario.getConnectorConfiguration().isEmpty()) {
                    connectorConfig.putAll(testScenario.getConnectorConfiguration());
                }

                // add the dynamic APIs as the schema registry and the firebolt account engine url
                Set<String> hostnames = new HashSet<>(testScenario.getStaticOutboundHostnames());
                hostnames.add(schemaRegistryUrl);
                hostnames.add(fireboltClient.getEngineUrl());
                List<String> networkEndpoints = createConnectorNetworkEndpoints(hostnames);
                log.info("Found : {} network endpoints", networkEndpoints);
                connectorConfig.put("confluent.custom.connection.endpoints", String.join(";", networkEndpoints));

                // create the connector
                Map<String, Object> createdConnectorConfig = confluentConnectorClient.createConnector(environmentId, clusterId, connectorName, fireboltPluginId, connectorConfig);
                createdConnectorConfig.entrySet().stream().forEach(entry -> log.info("Key: {}, [value] class: {},value  {} ", entry.getKey(), entry.getValue().getClass(), entry.getValue()));

                // wait for connector to be started successfully (It takes some time until the connector is provisioned)
                log.info("Waiting for connector to start");
                waitForConnectorToStart(confluentConnectorClient, connectorName);
                log.info("Connector {} is successfully running.", connectorName);

                // keep the connector id
                String connectorId = confluentConnectorClient.getConnectorId(connectorName);
                log.info("Connector id : {}", connectorId);

                // pause the connector (we will generate the messages first, and then we start the connector)
                pauseConnector(confluentConnectorClient, connectorName);

                String subjectName = topicName + "-value";
                registerJsonSchema(schemaRegistryClient, subjectName, testScenario.getJsonSchemaRegistryDefinitionFilePath());

                // start publishing messages
                publishMessages(testScenario.getNrOfKafkaMessageToProduce(), testRecordFactory, topicName, schemaRegistryUrl, schemaApiKey, schemaApiSecret, bootstrapServers, kafkaApiKey, kafkaApiSecret);

                // once all messages have been published start the connector
                startConnector(confluentConnectorClient, connectorName);

                // wait until all messages are ingested into firebolt
                waitForDataInFirebolt(fireboltClient, tableName);

                // verify and compute the ingestion details
                verifyFireboltRecords(fireboltClient, tableName);

                // collect some statistics from the run (how many messages per seconds were being inserted into firebolt, how many rows were inserted into each second, etc)
                LoadTestRunResult loadTestRunResult = collectAndPrintRunStats(fireboltClient, tableName, runStartTime);

                // stop connector
                pauseConnector(confluentConnectorClient, connectorName);

                // delete kafka topic
                confluentKafkaClient.deleteTopic(topicName);

                // delete schema
                schemaRegistryClient.deleteSubject(subjectName);

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

                if (Boolean.parseBoolean(testScenario.getConfluentCloudSettings().getExactlyOnce())) {
                    log.info("Dropping the table {}", KAFKA_METADATA_TABLE);
                    fireboltClient.dropTable(KAFKA_METADATA_TABLE);
                }

                return loadTestRunResult;
            }
        }
    }

    /**
     * This would be the basic default configuration.
     * @param schemaRegistryUrl
     * @return
     */
    private Map<String, String> createDefaultConnectorConfiguration(String schemaRegistryUrl) {
        String schemaApiKey = testScenario.getConfluentCloudSettings().getSchemaRegistryApiKey();
        String schemaApiSecret = testScenario.getConfluentCloudSettings().getSchemaRegistryApiSecret();
        String topicName = testScenario.getTopicName();
        String tableName = topicName;
        String jdbcUrl = testScenario.getFireboltSettings().getJdbcUrl();
        String fireboltClientId = testScenario.getFireboltSettings().getClientId();
        String fireboltClientSecret = testScenario.getFireboltSettings().getClientSecret();
        String kafkaApiKey = testScenario.getConfluentCloudSettings().getKafkaApiKey();
        String kafkaApiSecret = testScenario.getConfluentCloudSettings().getKafkaApiSecret();
        String exactlyOnce = testScenario.getConfluentCloudSettings().getExactlyOnce();
        

        Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("topic.to.table.mapping", topicName + ":" + tableName);
        connectorConfig.put("jdbc.connection.url", jdbcUrl);
        connectorConfig.put("value.converter.json.write.dates.iso8601", "true");
        connectorConfig.put("value.converter.schema.registry.url", schemaRegistryUrl);
        connectorConfig.put("poll.interval.ms", "1000");
        connectorConfig.put("value.converter.basic.auth.credentials.source", "USER_INFO");
        connectorConfig.put("value.converter.schema.registry.basic.auth.user.info", schemaApiKey+":"+schemaApiSecret);
        connectorConfig.put("fetch.max.bytes", "15000000");
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "io.confluent.connect.json.JsonSchemaConverter");
        connectorConfig.put("max.partition.fetch.bytes", "10000000");
        connectorConfig.put("producer.override.max.request.size", "10485760");
        connectorConfig.put("consumer.override.max.partition.fetch.bytes", "10485760");
        connectorConfig.put("consumer.override.fetch.max.bytes", "20971520");
        connectorConfig.put("connector.class", "com.firebolt.kafka.connect.FireboltSinkConnector");
        connectorConfig.put("tasks.max", "1");
        connectorConfig.put("kafka.api.key", kafkaApiKey);
        connectorConfig.put("kafka.api.secret", kafkaApiSecret);
        connectorConfig.put("firebolt.clientId", fireboltClientId);
        connectorConfig.put("firebolt.clientSecret", fireboltClientSecret);
        connectorConfig.put("errors.tolerance", "all");
        connectorConfig.put("errors.deadletterqueue.topic.name", "dlq-topic-firebolt");
        connectorConfig.put("errors.deadletterqueue.context.headers.enable", "true");
        connectorConfig.put("exactlyOnce", exactlyOnce);

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

        return LoadTestRunResult.builder()
                .fireboltIngestionRate(testScenario.getNrOfKafkaMessageToProduce() / fireboltIngestionDurationInSeconds)
                .fireboltTotalIngestionDuration(Duration.ofSeconds(fireboltIngestionDurationInSeconds))
                .queryHistoryDetails(queryHistoryResults)
                .build();
    }

    private List<String> getQueryHistoryResults(FireboltClient fireboltClient, String tableName, long runStartTimeInMillis, long runEndTimeInMillis) throws SQLException {
        String sql = "select query_id,submitted_time,start_time, end_time, duration_us, inserted_rows, inserted_bytes from information_schema.engine_query_history \n" +
                "  where query_text like 'INSERT INTO \"%s\" %%' \n" +  // need to escape the % so thus %%
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
                "  where query_text like 'INSERT INTO \"%s\" %%' \n" +  // need to escape the % so thus %%
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

    private void verifyFireboltRecords(FireboltClient client, String tableName) throws SQLException {
        int messageCount = testScenario.getNrOfKafkaMessageToProduce();

        // verify that the first record and last record is present
        List<Integer> recordIds = new ArrayList<>();
        recordIds.add(1);
        recordIds.add(messageCount);

        if (messageCount > 1000) {
            // if we have 50k messages produces, verify 50 records randomly
            int rowIdsToVerify = messageCount / 1000;

            // add another random 1000 record ids
            java.util.Set<Integer> added = new java.util.HashSet<>();
            java.util.Random rnd = new java.util.Random();

            while (added.size() < rowIdsToVerify) {
                // 1 and message count were already added by default.
                int val = 2 + rnd.nextInt(messageCount-1);
                if (!recordIds.contains(val)) {
                    added.add(val);
                }
            }

            recordIds.addAll(added);
        }

        log.info("Verifying {} record ids", recordIds.size());

        // verify rows in batches of 500
        int batchSize = 500;
        List<Integer> nextIds = new ArrayList<>();
        for (int i = 0;i<recordIds.size();i++) {
            nextIds.add(recordIds.get(i));

            if (nextIds.size() == batchSize) {
                log.info("Verifying a batch of ids");
                verifyIds(client, tableName, nextIds);

                nextIds = new ArrayList<>();
            }
        }

        if (!nextIds.isEmpty()) {
            log.info("Verifying the last batch");
            verifyIds(client, tableName, nextIds);
        }
    }

    private static void verifyIds(FireboltClient client, String tableName, List<Integer> ids) throws SQLException {
        ids = ids.stream().sorted().collect(Collectors.toList()); // natural sorting order is ascending
        StringBuilder sqlStatement = new StringBuilder("select \"colInteger\"")
                .append(" from \"").append(tableName).append("\" ")
                .append(" where \"colInteger\" in (");
        for (int i = 0; i<ids.size() -1; i++) {
            sqlStatement.append(ids.get(i)).append(",");
        }

        // append the last one
        sqlStatement.append(ids.get(ids.size()-1))
                .append(") order by \"colInteger\" asc;");  // order by ids ascending

        ResultSet resultSet = client.executeQuery(sqlStatement.toString());
        List<Integer> actualIds = new ArrayList<>();
        while (resultSet.next()) {
            actualIds.add(resultSet.getInt(1));
        }

        String idsVerified = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
        assertEquals("Mismatch in ids " + idsVerified, ids.size(), actualIds.size());
        assertEquals("Mismatch in ids " + idsVerified, ids, actualIds);
    }

    private void waitForDataInFirebolt(FireboltClient fireboltClient, String tableName) {
        log.info("Waiting for {} rows in Firebolt table '{}'...", testScenario.getNrOfKafkaMessageToProduce(), tableName);

        await()
                .atMost(testScenario.getFireboltIngestionWaitDuration())
                .pollInterval(Duration.ofSeconds(5))
                .until(() -> {
                    try {
                        int count = fireboltClient.countRows(tableName);
                        log.debug("Current row count in table '{}': {}", tableName, count);
                        return count >= testScenario.getNrOfKafkaMessageToProduce();
                    } catch (SQLException e) {
                        log.debug("Error querying Firebolt table: {}", e.getMessage());
                        return false;
                    }
                });

        log.info("Found expected data in Firebolt table '{}'", tableName);
    }

    private static void publishMessages(int messageCount, TestRecordFactory testRecordFactory, String topicName,
                                        String schemaEndpointUrl, String schemaApiKey, String schemaApiSecret,
                                        String bootstrapServers, String kafkaApiKey, String kafkaApiSecret) {
        // Publish a sample message that conforms to the all-data-types schema using JSON Schema producer
        try (Producer<String, LoadTestRecord> producer = initializeJsonProducer(
                true,
                bootstrapServers,
                schemaEndpointUrl,
                kafkaApiKey,
                kafkaApiSecret,
                schemaApiKey,
                schemaApiSecret)) {
            // Throughput improvements: async sends with batching & compression
            CountDownLatch latch = new CountDownLatch(messageCount);
            long start = System.currentTimeMillis();
            for (int i = 1; i <= messageCount; i++) {
                LoadTestRecord record = testRecordFactory.aValidRecord();
                ProducerRecord<String, LoadTestRecord> pr = new ProducerRecord<>(topicName, record.getColInteger().toString(), record);
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
            // Flush and wait bounded
            producer.flush();
            boolean completed = latch.await(Math.max(30L, messageCount / 100), TimeUnit.SECONDS);
            long tookMs = System.currentTimeMillis() - start;
            log.info("Published {} messages. Completed: {}. Elapsed: {} ms", messageCount, completed, tookMs);
        } catch (Exception e) {
            log.error("Failed to produce sample message", e);
        }
    }

    private static void registerJsonSchema(ConfluentSchemaRegistryClient schemaRegistryClient, String subject, String schemaPathName) throws IOException {
        String[] subjects = schemaRegistryClient.listSubjects();

        log.info("Found  {} subjects.", subjects.length);
        Arrays.stream(subjects).forEach(subjectName -> log.info("Subject: {}", subjectName));

        String jsonSchema = new String(Files.readAllBytes(Paths.get(schemaPathName)));

        int id = schemaRegistryClient.registerSchema(subject, jsonSchema, "JSON");
        log.info("Registered schema id {} for subject {}", id, subject);
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
        // if the connector is not paused, pause it
        String state = client.getConnectorState(connectorName);
        log.info("Found connector {} in state {}", connectorName, state);

        if ("paused".equalsIgnoreCase(state)) {
            log.info("Resuming the connector {}", connectorName);

            // pause the connector so we can change the configuration
            client.resumeConnector(connectorName);
            // actively wait for running state (max 60s, poll every 2s)
            waitForState(client, connectorName, "running");
        }

        log.info("The connector {} is in {} state", connectorName, state);
    }

    private static void pauseConnector(ConfluentConnectorClient client, String connectorName) throws IOException {
        // if the connector is not paused, pause it
        String state = client.getConnectorState(connectorName);
        log.info("Found connector {} in state {}", connectorName, state);

        if ("running".equalsIgnoreCase(state)) {
            log.info("Pausing the connector {}", connectorName);

            // pause the connector so we can change the configuration
            client.pauseConnector(connectorName);
            // actively wait for PAUSED state (max 60s, poll every 2s)
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
                log.debug("Waiting for connector to pause failed once: {}", e.getMessage());
                try { Thread.sleep(10000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }


    private static void waitForState(ConfluentConnectorClient client, String connectorName, String expectedState) {
        waitForState(client, connectorName, expectedState, Duration.ofMinutes(1));
    }

    // Local copy adapted from BaseIntegrationTest
    private static <T> Producer<String, T> initializeJsonProducer(
            boolean includeNulls,
            String bootstrapServers,
            String schemaRegistryUrl,
            String kafkaApiKey,
            String kafkaApiSecret,
            String srApiKey,
            String srApiSecret) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        // batching & compression for higher throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);            // small delay to batch
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64_000);       // ~64KB per batch
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // or "snappy"/"zstd"
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Confluent Cloud Kafka auth (SASL_SSL)
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + kafkaApiKey + "' password='" + kafkaApiSecret + "';");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", 45000);

        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", srApiKey + ":" + srApiSecret);
        props.put("auto.register.schemas", "false");
        props.put("use.latest.version", "true");
        props.put("latest.compatibility.strict", "false");

        props.put("json.oneof.for.nullables", includeNulls);
        props.put("json.default.property.inclusion", includeNulls ? "ALWAYS" : "NON_NULL");
        props.put("json.write.dates.iso8601", true);
        props.put("json.indent.output", false);

        return new KafkaProducer<>(props);
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
