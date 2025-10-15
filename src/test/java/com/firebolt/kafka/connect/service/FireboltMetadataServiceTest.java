package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
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
import java.util.Set;

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
        assertNotNull(metadataService.getLastOffsets(null, null));
        assertNotNull(metadataService.getLastOffsets(null, Set.of()));
        assertNotNull(metadataService.getLastOffsets("", null));
        assertNotNull(metadataService.getLastOffsets("", Set.of()));
        assertTrue(metadataService.getLastOffsets(null, null).isEmpty());
        assertTrue(metadataService.getLastOffsets(null, Set.of()).isEmpty());
        assertTrue(metadataService.getLastOffsets("", null).isEmpty());
        assertTrue(metadataService.getLastOffsets("", Set.of()).isEmpty());
    }

    @Test
    void getLastOffsets_shouldInsertMissingAndReturnZeroOffsets_whenNoneExist() throws Exception {
        // ensureAndGetOffsets: first prepareStatement is SELECT (no rows), second is INSERT
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        PreparedStatement insertPs = mock(PreparedStatement.class);

        when(connection.prepareStatement(startsWith("SELECT topic, topic_partition, partition_offset")))
            .thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        when(connection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPs);

        List<TopicPartitionOffsetDto> result = metadataService.getLastOffsets("t1", Set.of(0, 1));

        assertEquals(2, result.size());
        result.forEach(r -> assertEquals(0L, r.getOffset()));

        // verify batch insert invoked for two missing rows
        verify(insertPs, times(2)).addBatch();
        verify(insertPs, times(1)).executeBatch();
    }

    @Test
    void getLastOffsets_shouldReturnExistingAndInsertMissing() throws Exception {
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(startsWith("SELECT topic, topic_partition, partition_offset")))
            .thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        // one existing row (partition 0, offset 5), then end
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getInt("topic_partition")).thenReturn(0);
        when(resultSet.getLong("partition_offset")).thenReturn(5L);

        PreparedStatement insertPs = mock(PreparedStatement.class);
        when(connection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPs);

        List<TopicPartitionOffsetDto> result = metadataService.getLastOffsets("t1", Set.of(0, 1));

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

        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(connection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(updatePs);

        metadataService.updateOffsets(updates);

        // verify parameters were bound and batch executed
        verify(updatePs, times(2)).addBatch();
        verify(updatePs, times(1)).executeBatch();
        // no insert should be prepared in this path anymore
        verify(connection, never()).prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\""));
    }

    @Test
    void getLastOffsets_twiceWithUpdate_shouldNotDuplicateAndReturnUpdatedOffsets() throws Exception {
        // First call: SELECT returns no rows -> INSERT missing two -> return offsets [0,0]
        PreparedStatement selectPsFirst = mock(PreparedStatement.class);
        PreparedStatement insertPsLocal = mock(PreparedStatement.class);
        ResultSet rsFirst = mock(ResultSet.class);

        // Second call: SELECT returns two rows with updated offset for partition 1
        PreparedStatement selectPsSecond = mock(PreparedStatement.class);
        ResultSet rsSecond = mock(ResultSet.class);

        when(connection.prepareStatement(startsWith("SELECT topic, topic_partition, partition_offset")))
            .thenReturn(selectPsFirst, selectPsSecond);

        // First SELECT result: empty
        when(selectPsFirst.executeQuery()).thenReturn(rsFirst);
        when(rsFirst.next()).thenReturn(false);

        // INSERT prepared statement for first call
        when(connection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPsLocal);

        List<TopicPartitionOffsetDto> first = metadataService.getLastOffsets("topicB", Set.of(0, 1));
        assertEquals(2, first.size());
        assertTrue(first.stream().allMatch(o -> o.getOffset() == 0L));
        verify(insertPsLocal, times(2)).addBatch();
        verify(insertPsLocal, times(1)).executeBatch();

        // Update one offset to 7
        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(connection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(updatePs);
        metadataService.updateOffsets(List.of(
            TopicPartitionOffsetDto.builder().topic("topicB").partition(1).offset(7L).build()
        ));

        // Second SELECT result: two rows present: partition 0 -> 0, partition 1 -> 7
        when(selectPsSecond.executeQuery()).thenReturn(rsSecond);
        when(rsSecond.next()).thenReturn(true, true, false);
        when(rsSecond.getInt("topic_partition")).thenReturn(0, 1);
        when(rsSecond.getLong("partition_offset")).thenReturn(0L, 7L);

        // Second call should not perform any INSERTs
        List<TopicPartitionOffsetDto> second = metadataService.getLastOffsets("topicB", Set.of(0, 1));
        assertEquals(2, second.size());
        long p1Offset = second.stream().filter(o -> o.getPartition() == 1).findFirst().orElseThrow().getOffset();
        assertEquals(7L, p1Offset);

        // verify no additional inserts triggered in second call
        // total insert invocations remain from the first call only
        verify(insertPsLocal, times(2)).addBatch();
        verify(insertPsLocal, times(1)).executeBatch();
    }
}


