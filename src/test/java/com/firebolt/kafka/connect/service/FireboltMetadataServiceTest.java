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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
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
    private FireboltDbService mockFireboltDbService;

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private JdbcConfig mockJdbcConfig;
    private FireboltMetadataService metadataService;

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(mockFireboltDbService.createConnection(mockJdbcConfig)).thenReturn(mockConnection);
        lenient().when(mockConnection.createStatement()).thenReturn(mockStatement);
        lenient().when(mockStatement.executeUpdate(anyString())).thenReturn(0);

        metadataService = new FireboltMetadataService(mockFireboltDbService, mockJdbcConfig);
    }

    @Test
    void getLastOffsetsShouldThrow() {
        Set<Integer> emptySet = new HashSet<>();
        assertThrows(IllegalArgumentException.class,
                () -> metadataService.getLastOffsets(null, null));
        assertThrows(IllegalArgumentException.class,
                () -> metadataService.getLastOffsets(null, emptySet));
        assertThrows(IllegalArgumentException.class,
                () -> metadataService.getLastOffsets("", null));
        assertThrows(IllegalArgumentException.class,
                () -> metadataService.getLastOffsets("", emptySet));
    }

    @Test
    void getLastOffsetsShouldInsertMissingAndReturnZeroOffsetsWhenNoneExist() throws Exception {
        // ensureAndGetOffsets: first prepareStatement is SELECT (no rows), second is INSERT
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        PreparedStatement insertPs = mock(PreparedStatement.class);

        when(mockConnection.prepareStatement(anyString())).thenReturn(selectPs).thenReturn(insertPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        Map<Integer, Long> result = metadataService.getLastOffsets("t1", Set.of(0, 1));

        assertEquals(2, result.size());
        assertTrue(result.values().stream().allMatch(offset -> offset == -1));
        result.values().forEach(r -> assertEquals(-1L, r));

        // verify batch insert invoked for two missing rows
        verify(selectPs, times(1)).setString(1, "t1");
        verify(insertPs, times(2)).addBatch();
        verify(insertPs, times(1)).executeBatch();
    }

    @Test
    void getLastOffsetsShouldReturnExistingAndInsertMissing() throws Exception {
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(mockConnection.prepareStatement(argThat(sql ->
                sql.startsWith("SELECT topic, topic_partition, partition_offset FROM \"KafkaSinkConnectorMetadata\" WHERE topic = ? AND topic_partition in (") &&
                        (sql.contains("(0, 1)") || sql.contains("(1, 0)"))))).thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        // one existing row (partition 0, offset 5), then end
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getInt("topic_partition")).thenReturn(0);
        when(resultSet.getLong("partition_offset")).thenReturn(5L);

        PreparedStatement insertPs = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPs);

        Map<Integer, Long> result = metadataService.getLastOffsets("t1", Set.of(0, 1));

        assertEquals(2, result.size());
        Long existing = result.get(0);
        Long inserted = result.get(1);

        assertEquals(5L, existing);
        assertEquals(-1L, inserted);

        // verify only one insert for missing partition 1
        verify(insertPs, times(1)).setInt(1, 1);
        verify(insertPs, times(1)).addBatch();
        verify(insertPs, times(1)).executeBatch();
    }

    @Test
    void updateOffsetsShouldBatchUpdateOffsetsOnly() throws Exception {
        Map<Integer, Long> updates = Map.of(
            0, 10L,
            1, 20L
        );

        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(updatePs);

        metadataService.updateOffsets("t1", updates);

        // verify parameters were bound and batch executed
        verify(updatePs, times(2)).addBatch();
        verify(updatePs, times(1)).executeBatch();
        // no insert should be prepared in this path anymore
        verify(mockConnection, never()).prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\""));
    }

    @Test
    void getLastOffsetsTwiceWithUpdateShouldNotDuplicateAndReturnUpdatedOffsets() throws Exception {
        // First call: SELECT returns no rows -> INSERT missing two -> return offsets [0,0]
        PreparedStatement selectPsFirst = mock(PreparedStatement.class);
        PreparedStatement insertPsLocal = mock(PreparedStatement.class);
        ResultSet rsFirst = mock(ResultSet.class);

        // Second call: SELECT returns two rows with updated offset for partition 1
        PreparedStatement selectPsSecond = mock(PreparedStatement.class);
        ResultSet rsSecond = mock(ResultSet.class);

        when(mockConnection.prepareStatement(startsWith("SELECT topic, topic_partition, partition_offset")))
            .thenReturn(selectPsFirst, selectPsSecond);

        // First SELECT result: empty
        when(selectPsFirst.executeQuery()).thenReturn(rsFirst);
        when(rsFirst.next()).thenReturn(false);

        // INSERT prepared statement for first call
        when(mockConnection.prepareStatement(startsWith("INSERT INTO \"KafkaSinkConnectorMetadata\"")))
            .thenReturn(insertPsLocal);

        Map<Integer, Long> first = metadataService.getLastOffsets("topicB", Set.of(0, 1));
        assertEquals(2, first.size());
        assertTrue(first.values().stream().allMatch(offset -> offset == -1));
        verify(insertPsLocal, times(2)).addBatch();
        verify(insertPsLocal, times(1)).executeBatch();

        // Update one offset to 7
        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
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


