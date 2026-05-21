package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This service manages the Kafka related metadata in Firebolt: topic, partition, offsets.
 * It ensures that the metadata table exists, retrieves the last committed offsets for given topic-partitions,
 * and updates offsets after records are processed.
 * The metadata is stored in a table named "KafkaSinkConnectorMetadata"
 * which will be created if it does not exist with columns:
 * - topic (TEXT)
 * - topic_partition (INTEGER)
 * - partition_offset (BIGINT)
 */
@Slf4j
public class FireboltMetadataService {

    private static final String METADATA_TABLE_NAME = "KafkaSinkConnectorMetadata";
    private static final long DEFAULT_OFFSET = -1;
    private static final String SELECT_QUERY =
            String.format("SELECT topic, topic_partition, partition_offset FROM \"%s\" WHERE topic = ? AND topic_partition in ", METADATA_TABLE_NAME);
    private static final String CREATE_TABLE_QUERY =
            String.format("CREATE TABLE IF NOT EXISTS \"%s\" (topic TEXT, topic_partition INTEGER, partition_offset BIGINT)", METADATA_TABLE_NAME);
    private final FireboltDbService fireboltDbService;
    private final JdbcConfig jdbcConfig;

    public FireboltMetadataService(FireboltDbService fireboltDbService, JdbcConfig jdbcConfig) {
        this.fireboltDbService = fireboltDbService;
        this.jdbcConfig = jdbcConfig;
        ensureMetadataTableExists();
    }

    /**
     * @param topicName: the topic name
     * @param topicPartitions: the set of topic partitions
     * @return a map of topic partition to last committed offset
     * @throws IllegalArgumentException if topicName is blank or topicPartitions is empty
     * @throws RuntimeException if there is an error querying or inserting into the metadata table
     */
    public Map<Integer, Long> getLastOffsets(String topicName, Set<Integer> topicPartitions) {
        if (StringUtils.isBlank(topicName) || CollectionUtils.isEmpty(topicPartitions)) {
            log.error("Invalid topicName: {}, or partitions {}", topicName, topicPartitions);
            throw new IllegalArgumentException("Topic name or partitions cannot be empty");
        }
        Map<Integer, Long> results = new HashMap<>();

        String selectQuery = SELECT_QUERY + createPartitionList(topicPartitions);
        try (Connection connection = fireboltDbService.createConnection(jdbcConfig);
             PreparedStatement selectPs = connection.prepareStatement(selectQuery);) {
            selectPs.setString(1, topicName);

            Set<Integer> presentTopics = new HashSet<>();
            try (ResultSet rs = selectPs.executeQuery()) {
                while (rs.next()) {
                    int partition = rs.getInt("topic_partition");
                    long offset = rs.getLong("partition_offset");
                    presentTopics.add(partition);
                    results.put(partition, offset);
                }
            }

            if (presentTopics.size() == topicPartitions.size()) {
                return results;
            }

            String insertSql = String.format("INSERT INTO \"%s\" (topic, topic_partition, partition_offset) VALUES (?, ?, ?)",
                    METADATA_TABLE_NAME);
            try (PreparedStatement insertPs = connection.prepareStatement(insertSql)) {
                for (Integer partition : topicPartitions) {
                    if (!presentTopics.contains(partition)) {
                        insertPs.setString(1, topicName);
                        insertPs.setInt(2, partition);
                        insertPs.setLong(3, DEFAULT_OFFSET);
                        insertPs.addBatch();

                        results.put(partition, DEFAULT_OFFSET);
                    }
                }
                insertPs.executeBatch();
            }

            return results;

        } catch (SQLException e) {
            log.error("Failed to ensure offsets exist in metadata table: {}", METADATA_TABLE_NAME, e);
            throw new RuntimeException("Failed to ensure offsets exist in metadata table: " + METADATA_TABLE_NAME, e);
        }
    }

    public void updateOffsets(String topicName, Map<Integer, Long> offsets) {
        if (StringUtils.isBlank(topicName) || MapUtils.isEmpty(offsets)) {
            log.error("Invalid topic: {}, or offsets {}", topicName, offsets);
            throw new IllegalArgumentException("Topic name or offsets cannot be empty");
        }

        String updateSql = String.format("UPDATE \"%s\" SET partition_offset = ? WHERE topic = ? AND topic_partition = ?",
                METADATA_TABLE_NAME);

        try (Connection connection = fireboltDbService.createConnection(jdbcConfig);
             PreparedStatement updatePs = connection.prepareStatement(updateSql)) {

            for (Map.Entry<Integer, Long> offsetByPartition: offsets.entrySet()) {
                Integer partition = offsetByPartition.getKey();
                Long offset = offsetByPartition.getValue();

                updatePs.setLong(1, offset);
                updatePs.setString(2, topicName);
                updatePs.setInt(3, partition);

                updatePs.addBatch();
            }
            updatePs.executeBatch();

        } catch (SQLException e) {
            //not authorized exceptions come back as 400 s from firebolt so we can't really differentiate here
            log.error("Failed to update offsets in metadata table: {}", METADATA_TABLE_NAME, e);
            throw new RuntimeException("Failed to update offsets in metadata table: " + METADATA_TABLE_NAME, e);
        }
    }

    private void ensureMetadataTableExists() {
        try (Connection connection = fireboltDbService.createConnection(jdbcConfig);
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(CREATE_TABLE_QUERY);
        } catch (SQLException e) {
            log.error("Failed to ensure metadata table exists: {}", METADATA_TABLE_NAME, e);
            throw new RuntimeException("Failed to ensure metadata table exists: " + METADATA_TABLE_NAME, e);
        }
    }

    private String createPartitionList(Set<Integer> topicPartitions) {
        return topicPartitions.stream().map(String::valueOf).collect(Collectors.joining(", ", "(", ")"));
    }

}
