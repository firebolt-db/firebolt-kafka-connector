package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.service.dto.TopicPartitionDto;
import com.firebolt.kafka.connect.service.dto.TopicPartitionOffsetDto;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This service manages the Kafka related metadata in Firebolt: topic, partition, offsets.
 * It ensures that the metadata table exists, retrieves the last committed offsets for given topic-partitions,
 * and updates offsets after records are processed.
 * The metadata is stored in a table named "KafkaSinkConnectorMetadata" with columns:
 * - topic (TEXT)
 * - topic_partition (INTEGER)
 * - partition_offset (BIGINT)
 */
@Slf4j
@RequiredArgsConstructor
public class FireboltMetadataService {

    private static final String METADATA_TABLE_NAME = "KafkaSinkConnectorMetadata";

    private final FireboltDbService fireboltDbService;
    private final JdbcConfig jdbcConfig;

    List<TopicPartitionOffsetDto> getLastOffsets(Map<String, List<Integer>> topicPartitions) {
        return ensureAndGetOffsets(topicPartitions);
    }

    void updateOffsets(List<TopicPartitionOffsetDto> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return;
        }

        ensureMetadataTableExists();

        String updateSql = "UPDATE \"" + METADATA_TABLE_NAME +
            "\" SET partition_offset = ? WHERE topic = ? AND topic_partition = ?";

        try (Connection connection = fireboltDbService.createConnection(jdbcConfig);
             PreparedStatement updatePs = connection.prepareStatement(updateSql)) {

            for (TopicPartitionOffsetDto offsetDto : offsets) {
                updatePs.setLong(1, offsetDto.getOffset());
                updatePs.setString(2, offsetDto.getTopic());
                updatePs.setInt(3, offsetDto.getPartition());

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

            String createSql = "CREATE TABLE IF NOT EXISTS \"" + METADATA_TABLE_NAME +
                    "\" (topic TEXT, topic_partition INTEGER, partition_offset BIGINT)";
            statement.executeUpdate(createSql);

        } catch (SQLException e) {
            log.error("Failed to ensure metadata table exists: {}", METADATA_TABLE_NAME, e);
            throw new RuntimeException("Failed to ensure metadata table exists: " + METADATA_TABLE_NAME, e);
        }
    }

    private List<TopicPartitionOffsetDto> ensureAndGetOffsets(Map<String, List<Integer>> topicPartitions) {
        long defaultOffset = 0;
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            return Collections.emptyList();
        }

        ensureMetadataTableExists();

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT topic, topic_partition, partition_offset FROM \"")
            .append(METADATA_TABLE_NAME)
            .append("\" WHERE ");

        for (int i = 0; i < topicPartitions.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(" OR ");
            }
            sqlBuilder.append("(topic = ? AND topic_partition in ?)");
        }

        String querySql = sqlBuilder.toString();

        List<TopicPartitionOffsetDto> results = new ArrayList<>();

        try (Connection connection = fireboltDbService.createConnection(jdbcConfig);
             PreparedStatement selectPs = connection.prepareStatement(querySql)) {

            int paramIndex = 1;
            for (Map.Entry<String, List<Integer>> topicPartition : topicPartitions.entrySet()) {
                selectPs.setString(paramIndex++, topicPartition.getKey());
                selectPs.setString(paramIndex++, createPartitionList(topicPartition.getValue()));
            }

            Set<String> presentKeys = new HashSet<>();
            try (ResultSet rs = selectPs.executeQuery()) {
                while (rs.next()) {
                    String topic = rs.getString("topic");
                    int partition = rs.getInt("topic_partition");
                    long offset = rs.getLong("partition_offset");
                    presentKeys.add(topic + "#" + partition);
                    results.add(TopicPartitionOffsetDto.builder()
                        .topic(topic)
                        .partition(partition)
                        .offset(offset)
                        .build());
                }
            }

            String insertSql = "INSERT INTO \"" + METADATA_TABLE_NAME +
                "\" (topic, topic_partition, partition_offset) VALUES (?, ?, " + defaultOffset + ")";
            try (PreparedStatement insertPs = connection.prepareStatement(insertSql)) {
                boolean hasInserts = false;
                for (Map.Entry<String, List<Integer>> topicPartition : topicPartitions.entrySet()) {
                    for (Integer partition : topicPartition.getValue()) {
                        String topic = topicPartition.getKey();
                        String key = topic + "#" + partition;
                        if (!presentKeys.contains(key)) {
                            insertPs.setString(1, topic);
                            insertPs.setInt(2, partition);
                            insertPs.addBatch();
                            hasInserts = true;

                            results.add(TopicPartitionOffsetDto.builder()
                                .topic(topic)
                                .partition(partition)
                                .offset(defaultOffset)
                                .build());
                        }
                    }
                }
                if (hasInserts) {
                    insertPs.executeBatch();
                }
            }

            return results;

        } catch (SQLException e) {
            log.error("Failed to ensure offsets exist in metadata table: {}", METADATA_TABLE_NAME, e);
            throw new RuntimeException("Failed to ensure offsets exist in metadata table: " + METADATA_TABLE_NAME, e);
        }
    }

    private String createPartitionList(List<Integer> value) {
        return value.stream().map(String::valueOf).collect(Collectors.joining(", ", "(", ")"));
    }

}
