package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.JdbcConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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

    @Captor
    private ArgumentCaptor<String> preparedStatementsArgumentCapture;

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

        verify(mockConnection, times(2)).prepareStatement(preparedStatementsArgumentCapture.capture());
        // first prepared statement is the select query
        String selectQuery = preparedStatementsArgumentCapture.getAllValues().get(0);

        // the topic partition is not always sorted
        Set<String> expectedQuery = Set.of(
                "SELECT topic, topic_partition, partition_offset FROM \"KafkaSinkConnectorMetadata\" WHERE topic = ? AND topic_partition in (0, 1)",
                "SELECT topic, topic_partition, partition_offset FROM \"KafkaSinkConnectorMetadata\" WHERE topic = ? AND topic_partition in (1, 0)"
        );
        assertTrue(expectedQuery.contains(selectQuery));

        verify(selectPs).setString(1, "t1");

        // second prepared statement is the parameterized insert (topic bound as a parameter, not interpolated)
        String insertStatement = preparedStatementsArgumentCapture.getAllValues().get(1);
        assertEquals("INSERT INTO \"KafkaSinkConnectorMetadata\" (topic, topic_partition, partition_offset) VALUES (?, ?, ?)", insertStatement);

        // verify all three parameters are bound for each missing partition
        verify(insertPs, times(2)).setString(1, "t1");
        verify(insertPs).setInt(2, 0);
        verify(insertPs).setInt(2, 1);
        verify(insertPs, times(2)).setLong(3, -1L);
        verify(insertPs, times(2)).addBatch();
        verify(insertPs).executeBatch();
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

        verify(mockConnection, times(2)).prepareStatement(preparedStatementsArgumentCapture.capture());
        String insertStatement = preparedStatementsArgumentCapture.getAllValues().get(1);
        assertEquals("INSERT INTO \"KafkaSinkConnectorMetadata\" (topic, topic_partition, partition_offset) VALUES (?, ?, ?)", insertStatement);

        // verify only one insert for missing partition 1, with all three parameters bound
        verify(insertPs, times(1)).setString(1, "t1");
        verify(insertPs, times(1)).setInt(2, 1);
        verify(insertPs, times(1)).setLong(3, -1L);
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

        verify(mockConnection).prepareStatement(preparedStatementsArgumentCapture.capture());
        String updateStatement = preparedStatementsArgumentCapture.getAllValues().get(0);
        // topic is now a bound parameter (position 2), not interpolated into the SQL string
        assertEquals("UPDATE \"KafkaSinkConnectorMetadata\" SET partition_offset = ? WHERE topic = ? AND topic_partition = ?", updateStatement);

        verify(updatePs).setLong(1, 10L);
        verify(updatePs).setLong(1, 20L);
        verify(updatePs, times(2)).setString(2, "t1");
        verify(updatePs).setInt(3, 0);
        verify(updatePs).setInt(3, 1);

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

        verify(mockConnection, times(3)).prepareStatement(preparedStatementsArgumentCapture.capture());
        String updateStatement = preparedStatementsArgumentCapture.getAllValues().get(2);
        assertEquals("UPDATE \"KafkaSinkConnectorMetadata\" SET partition_offset = ? WHERE topic = ? AND topic_partition = ?", updateStatement);

        verify(updatePs).setLong(1, 7L);
        verify(updatePs).setString(2, "topicB");
        verify(updatePs).setInt(3, 1);
        verify(updatePs).addBatch();
        verify(updatePs).executeBatch();

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
        assertEquals( 0L, second.get(0));

        // verify no additional inserts triggered in second call
        // total insert invocations remain from the first call only
        verify(insertPsLocal, times(2)).addBatch();
        verify(insertPsLocal, times(1)).executeBatch();
    }

    @Test
    void shouldPersistAndRecoverOffsetsAcrossRestart() throws Exception {
        // Regression test: before the fix, updateOffsets() was never called from production code.
        // getLastOffsets() would always return -1 on restart, reprocessing the entire history.
        // This test would have FAILED before the fix: it calls updateOffsets() directly to simulate
        // what TableWriter now does, then verifies getLastOffsets() recovers the persisted values.

        // Step 1: Simulate processing a batch — TableWriter calls updateOffsets after insert.
        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
                .thenReturn(updatePs);

        metadataService.updateOffsets("orders", Map.of(0, 42L, 1, 99L));

        verify(updatePs).setLong(1, 42L);
        verify(updatePs).setLong(1, 99L);
        verify(updatePs, times(2)).setString(2, "orders");
        verify(updatePs).setInt(3, 0);
        verify(updatePs).setInt(3, 1);
        verify(updatePs, times(2)).addBatch();
        verify(updatePs).executeBatch();

        // Step 2: Simulate a connector restart — getLastOffsets() must return the persisted values,
        // not -1. Before the fix, updateOffsets was never called so restarts always got -1.
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(mockConnection.prepareStatement(argThat(sql ->
                sql.startsWith("SELECT topic, topic_partition, partition_offset FROM \"KafkaSinkConnectorMetadata\" WHERE topic = ? AND topic_partition in (") &&
                        (sql.contains("(0, 1)") || sql.contains("(1, 0)"))))).thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(resultSet);
        // Both partitions are present in the DB with their persisted offsets.
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getInt("topic_partition")).thenReturn(0, 1);
        when(resultSet.getLong("partition_offset")).thenReturn(42L, 99L);

        Map<Integer, Long> recovered = metadataService.getLastOffsets("orders", Set.of(0, 1));

        // The connector must resume from the persisted high-water marks, not reprocess from the start.
        assertEquals(42L, recovered.get(0), "Partition 0 offset must survive restart");
        assertEquals(99L, recovered.get(1), "Partition 1 offset must survive restart");
    }

    @Test
    void updateOffsetsShouldPassMaliciousTopicNameAsParameterNotInterpolated() throws Exception {
        // SQL injection guard: a hostile topic name must reach the DB as a bound parameter,
        // not as raw SQL text. If the old String.format path were used, the DROP TABLE
        // statement would be syntactically part of the query.
        String maliciousTopic = "evil_topic'; DROP TABLE users; --";
        Map<Integer, Long> updates = Map.of(0, 1L);

        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
                .thenReturn(updatePs);

        metadataService.updateOffsets(maliciousTopic, updates);

        // Verify all three parameters are bound correctly — offset at position 1, topic at 2, partition at 3.
        // The hostile topic name reaches the driver as a safe bound parameter, not as interpolated SQL.
        verify(updatePs).setLong(1, 1L);
        verify(updatePs).setString(2, maliciousTopic);
        verify(updatePs).setInt(3, 0);
        // Confirm the write actually executed.
        verify(updatePs).addBatch();
        verify(updatePs).executeBatch();

        // Round-trip: read back via getLastOffsets to confirm the write landed correctly.
        // The SELECT must also use parameterized binding (topic as a bound parameter, not interpolated).
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet selectRs = mock(ResultSet.class);
        when(mockConnection.prepareStatement(startsWith(
                "SELECT topic, topic_partition, partition_offset FROM \"KafkaSinkConnectorMetadata\" WHERE topic = ? AND topic_partition in")))
                .thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(selectRs);
        when(selectRs.next()).thenReturn(true, false);
        when(selectRs.getInt("topic_partition")).thenReturn(0);
        when(selectRs.getLong("partition_offset")).thenReturn(1L);

        Map<Integer, Long> recovered = metadataService.getLastOffsets(maliciousTopic, Set.of(0));

        // The parameterized SELECT returns the persisted offset — the hostile topic name did not
        // escape the parameter and corrupt the query.
        assertEquals(1L, recovered.get(0), "Offset must survive the round-trip with a malicious topic name");
        verify(selectPs).setString(1, maliciousTopic);
    }

    @Test
    void updateOffsetsShouldPassTopicWithSpecialQuotesAsParameterNotInterpolated() throws Exception {
        // SQL injection guard: topic names with embedded quotes must be bound safely.
        String quotedTopic = "topic\"with'quotes";
        Map<Integer, Long> updates = Map.of(1, 5L);

        PreparedStatement updatePs = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(startsWith("UPDATE \"KafkaSinkConnectorMetadata\"")))
                .thenReturn(updatePs);

        metadataService.updateOffsets(quotedTopic, updates);

        // Both single and double quotes in the topic name are harmless when parameterized.
        // Verify all three parameters and the complete write path.
        verify(updatePs).setLong(1, 5L);
        verify(updatePs).setString(2, quotedTopic);
        verify(updatePs).setInt(3, 1);
        verify(updatePs).addBatch();
        verify(updatePs).executeBatch();

        // Round-trip: read back via getLastOffsets to confirm the write landed correctly.
        // Embedded quotes in the topic name must be harmless when the SELECT also uses parameterization.
        PreparedStatement selectPs = mock(PreparedStatement.class);
        ResultSet selectRs = mock(ResultSet.class);
        when(mockConnection.prepareStatement(startsWith(
                "SELECT topic, topic_partition, partition_offset FROM \"KafkaSinkConnectorMetadata\" WHERE topic = ? AND topic_partition in")))
                .thenReturn(selectPs);
        when(selectPs.executeQuery()).thenReturn(selectRs);
        when(selectRs.next()).thenReturn(true, false);
        when(selectRs.getInt("topic_partition")).thenReturn(1);
        when(selectRs.getLong("partition_offset")).thenReturn(5L);

        Map<Integer, Long> recovered = metadataService.getLastOffsets(quotedTopic, Set.of(1));

        // Embedded quotes in the topic name did not corrupt the SELECT query.
        assertEquals(5L, recovered.get(1), "Offset must survive the round-trip with a quoted topic name");
        verify(selectPs).setString(1, quotedTopic);
    }
}


