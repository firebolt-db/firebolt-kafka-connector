package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import com.firebolt.kafka.connect.service.dto.TopicPartitionDto;
import com.firebolt.kafka.connect.service.dto.TopicPartitionOffsetDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltMetadataServiceTest {

    @Mock
    private FireboltDbService fireboltDbService;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private PreparedStatement selectPs;

    @Mock
    private PreparedStatement insertPs;

    @Mock
    private PreparedStatement updatePs;

    @Mock
    private ResultSet resultSet;

    private JdbcConfig jdbcConfig;
    private FireboltMetadataService metadataService;

    @BeforeEach
    void setUp() throws Exception {
        jdbcConfig = JdbcConfig.builder()
            .jdbcConnectionUrl("jdbc:firebolt:test_db")
            .clientId(Optional.empty())
            .clientSecret(Optional.empty())
            .build();
        metadataService = new FireboltMetadataService(fireboltDbService, jdbcConfig);

        lenient().when(fireboltDbService.createConnection(any())).thenReturn(connection);
        lenient().when(connection.createStatement()).thenReturn(statement);
        // Default: CREATE TABLE IF NOT EXISTS always succeeds
        lenient().when(statement.executeUpdate(anyString())).thenReturn(0);
    }

    @Test
    void getLastOffsets_shouldReturnEmpty_whenNullOrEmptyInput() {
        assertNotNull(metadataService.getLastOffsets(null));
        assertTrue(metadataService.getLastOffsets(null).isEmpty());
        assertTrue(metadataService.getLastOffsets(List.of()).isEmpty());
    }

    @Test
    void getLastOffsets_shouldInsertMissingAndReturnZeroOffsets_whenNoneExist() throws Exception {
        List<TopicPartitionDto> tps = Arrays.asList(
            TopicPartitionDto.builder().topic("t1").partition(0).build(),
            TopicPartitionDto.builder().topic("t1").partition(1).build()
        );

        // ensureAndGetOffsets: first prepareStatement is SELECT (no rows), second is INSERT
        when(connection.prepareStatement(startsWith("SELECT topic, topic_partition, partition_offset")))
            .thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        when(connection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPs);

        List<TopicPartitionOffsetDto> result = metadataService.getLastOffsets(tps);

        assertEquals(2, result.size());
        result.forEach(r -> assertEquals(0L, r.getOffset()));

        // verify batch insert invoked for two missing rows
        verify(insertPs, times(2)).addBatch();
        verify(insertPs, times(1)).executeBatch();
    }

    @Test
    void getLastOffsets_shouldReturnExistingAndInsertMissing() throws Exception {
        List<TopicPartitionDto> tps = Arrays.asList(
            TopicPartitionDto.builder().topic("t1").partition(0).build(),
            TopicPartitionDto.builder().topic("t1").partition(1).build()
        );

        when(connection.prepareStatement(startsWith("SELECT topic, topic_partition, partition_offset")))
            .thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        // one existing row (partition 0, offset 5), then end
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("topic")).thenReturn("t1");
        when(resultSet.getInt("topic_partition")).thenReturn(0);
        when(resultSet.getLong("partition_offset")).thenReturn(5L);

        when(connection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPs);

        List<TopicPartitionOffsetDto> result = metadataService.getLastOffsets(tps);

        assertEquals(2, result.size());
        TopicPartitionOffsetDto existing = result.stream().filter(r -> r.getPartition() == 0).findFirst().orElseThrow();
        TopicPartitionOffsetDto inserted = result.stream().filter(r -> r.getPartition() == 1).findFirst().orElseThrow();

        assertEquals(5L, existing.getOffset());
        assertEquals(0L, inserted.getOffset());

        // verify only one insert for missing partition 1
        verify(insertPs, times(1)).addBatch();
        verify(insertPs, times(1)).executeBatch();
    }

    @Test
    void updateOffsets_shouldBatchUpdateOffsetsOnly() throws Exception {
        List<TopicPartitionOffsetDto> updates = Arrays.asList(
            TopicPartitionOffsetDto.builder().topic("t1").partition(0).offset(10L).build(),
            TopicPartitionOffsetDto.builder().topic("t1").partition(1).offset(20L).build()
        );

        when(connection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(updatePs);

        metadataService.updateOffsets(updates);

        // verify parameters were bound and batch executed
        verify(updatePs, times(2)).addBatch();
        verify(updatePs, times(1)).executeBatch();
        // no insert should be prepared in this path anymore
        verify(connection, never()).prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\""));
    }
}


