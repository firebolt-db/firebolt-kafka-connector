package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        Map<Integer, Long> result = metadataService.getLastOffsets("t1", Set.of(0, 1));

        assertEquals(2, result.size());
        assertTrue(result.values().stream().allMatch(offset -> offset == 0));
        result.values().forEach(r -> assertEquals(0L, r));

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

        Map<Integer, Long> result = metadataService.getLastOffsets("t1", Set.of(0, 1));

        assertEquals(2, result.size());
        Long existing = result.get(0);
        Long inserted = result.get(1);

        assertEquals(5L, existing);
        assertEquals(0L, inserted);

        // verify only one insert for missing partition 1
        verify(insertPs, times(1)).addBatch();
        verify(insertPs, times(1)).executeBatch();
    }

    @Test
    void updateOffsets_shouldBatchUpdateOffsetsOnly() throws Exception {
        Map<Integer, Long> updates = Map.of(
            0, 10L,
            1, 20L
        );

        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(connection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(updatePs);

        metadataService.updateOffsets("t1", updates);

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

        Map<Integer, Long> first = metadataService.getLastOffsets("topicB", Set.of(0, 1));
        assertEquals(2, first.size());
        assertTrue(first.values().stream().allMatch(offset -> offset == 0));
        verify(insertPsLocal, times(2)).addBatch();
        verify(insertPsLocal, times(1)).executeBatch();

        // Update one offset to 7
        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(connection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(updatePs);
        metadataService.updateOffsets("topicB", Map.of(1, 7L));

        // Second SELECT result: two rows present: partition 0 -> 0, partition 1 -> 7
        when(selectPsSecond.executeQuery()).thenReturn(rsSecond);
        when(rsSecond.next()).thenReturn(true, true, false);
        when(rsSecond.getInt("topic_partition")).thenReturn(0, 1);
        when(rsSecond.getLong("partition_offset")).thenReturn(0L, 7L);

        // Second call should not perform any INSERTs
        Map<Integer, Long> second = metadataService.getLastOffsets("topicB", Set.of(0, 1));
        assertEquals(2, second.size());
        long p1Offset = second.get(1);
        assertEquals(7L, p1Offset);

        // verify no additional inserts triggered in second call
        // total insert invocations remain from the first call only
        verify(insertPsLocal, times(2)).addBatch();
        verify(insertPsLocal, times(1)).executeBatch();
    }
}


