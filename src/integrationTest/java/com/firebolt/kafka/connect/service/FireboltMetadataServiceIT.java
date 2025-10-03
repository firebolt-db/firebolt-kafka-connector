package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.integration.BaseIntegrationTest;
import com.firebolt.kafka.connect.service.dto.TopicPartitionDto;
import com.firebolt.kafka.connect.service.dto.TopicPartitionOffsetDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FireboltMetadataServiceIT extends BaseIntegrationTest {

    private FireboltClient client;
    private FireboltDbService dbService;
    private FireboltMetadataService metadataService;
    private JdbcConfig jdbcConfig;

    @BeforeEach
    void init() throws SQLException {
        client = FireboltClient.createFor(getDatabaseName());
        dbService = new FireboltDbService();
        jdbcConfig = JdbcConfig.builder()
            .jdbcConnectionUrl(getJdbcConnectionUrl("http://localhost:3473"))
            .clientId(Optional.ofNullable(getClientId()))
            .clientSecret(Optional.ofNullable(getClientSecret()))
            .build();
        metadataService = new FireboltMetadataService(dbService, jdbcConfig);

        // ensure clean metadata table
        client.executeUpdate("DROP TABLE IF EXISTS \"KafkaSinkConnectorMetadata\"");
    }

    @AfterEach
    void cleanup() throws SQLException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void shouldInsertMissingAndReturnOffsets() throws Exception {
        List<TopicPartitionDto> tps = List.of(
            TopicPartitionDto.builder().topic("topicA").partition(0).build(),
            TopicPartitionDto.builder().topic("topicA").partition(1).build()
        );

        List<TopicPartitionOffsetDto> offsets = metadataService.getLastOffsets(tps);

        assertEquals(2, offsets.size());
        assertTrue(offsets.stream().allMatch(o -> o.getOffset() == 0L));

        // verify rows present in DB
        try (ResultSet rs = client.executeQuery("SELECT count(*) as c FROM \"KafkaSinkConnectorMetadata\"")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("c"));
        }
    }

    @Test
    void shouldReturnExistingAndNotDuplicateOnSecondCall() throws Exception {
        List<TopicPartitionDto> tps = List.of(
            TopicPartitionDto.builder().topic("topicB").partition(0).build(),
            TopicPartitionDto.builder().topic("topicB").partition(1).build()
        );

        List<TopicPartitionOffsetDto> first = metadataService.getLastOffsets(tps);
        assertEquals(2, first.size());

        // update one offset and persist
        metadataService.updateOffsets(List.of(
            TopicPartitionOffsetDto.builder().topic("topicB").partition(1).offset(7L).build()
        ));

        List<TopicPartitionOffsetDto> second = metadataService.getLastOffsets(tps);
        assertEquals(2, second.size());
        long partition1Offset = second.stream().filter(o -> o.getPartition() == 1).findFirst().orElseThrow().getOffset();
        assertEquals(7L, partition1Offset);

        // verify still two rows
        try (ResultSet rs = client.executeQuery("SELECT count(*) as c FROM \"KafkaSinkConnectorMetadata\" WHERE topic='topicB'")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("c"));
        }
    }
}


