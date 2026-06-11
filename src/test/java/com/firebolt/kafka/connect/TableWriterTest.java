package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltMetadataService;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableWriterTest {

    private static final String TABLE_NAME = "test_table";
    private static final String TOPIC_NAME = "test-topic";

    private static final Integer PARTITION_0 = 0;
    private static final Integer PARTITION_1 = 1;
    private static final Integer PARTITION_2 = 2;

    @Mock
    private TableSchema mockTableSchema;

    @Mock
    private IngestionService mockIngestionService;

    @Mock
    private FireboltMetadataService mockFireboltMetadataService;

    private TableWriter tableWriter;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        when(mockTableSchema.getTableName()).thenReturn(TABLE_NAME);

        Map<Integer, Long> lastPartitionOffset = new HashMap<>();
        lastPartitionOffset.put(PARTITION_0,  -1L);
        lastPartitionOffset.put(PARTITION_1,  -1L);
        lastPartitionOffset.put(PARTITION_2,  -1L);
        tableWriter = new TableWriter(mockTableSchema, mockFireboltMetadataService, TOPIC_NAME,  lastPartitionOffset, mockIngestionService);

        doNothing().when(mockIngestionService).addRecords(anyList());
    }

    @Test
    void shouldInitializeWithDefaultProcessedPartitionOffsets() {
        Map<Integer, Long> processedPartitionOffsets = tableWriter.getProcessedPartitionOffsets();
        assertNotNull(processedPartitionOffsets);
        assertFalse(processedPartitionOffsets.isEmpty());
        assertEquals(-1L, processedPartitionOffsets.get(0));
        assertEquals(-1L, processedPartitionOffsets.get(1));
        assertEquals(-1L, processedPartitionOffsets.get(2));

        // should not be able to modify the offsets outside the class
        assertThrows(RuntimeException.class, () -> processedPartitionOffsets.put(1, 1L));
    }

    @Test
    void shouldInsertRecordsSuccessfully() throws SQLException {
        List<SinkRecord> records = List.of(
                buildRecord(0, 100L),
                buildRecord(1, 200L),
                buildRecord(2, 300L));
        doNothing().when(mockIngestionService).addRecords(records);
        assertDoesNotThrow(() -> tableWriter.insertRecords(records));

        Map<Integer, Long> offsets = tableWriter.getProcessedPartitionOffsets();
        assertEquals(3, offsets.size());
        assertEquals(100L, offsets.get(0));
        assertEquals(200L, offsets.get(1));
        assertEquals(300L, offsets.get(2));
    }

    @Test
    void shouldInsertRecordsSuccessfullyWhenAllRecordsBelongToOnePartition() throws SQLException {
        List<SinkRecord> records = List.of(
                buildRecord(0, 100L),
                buildRecord(0, 101L),
                buildRecord(0, 102L));
        doNothing().when(mockIngestionService).addRecords(records);
        assertDoesNotThrow(() -> tableWriter.insertRecords(records));

        Map<Integer, Long> offsets = tableWriter.getProcessedPartitionOffsets();
        assertEquals(3, offsets.size());
        assertEquals(102L, offsets.get(0));
        assertEquals(-1L, offsets.get(1));
        assertEquals(-1L, offsets.get(2));
    }


    @Test
    void shouldHandleEmptyRecordsList() throws Exception {
        List<SinkRecord> emptyRecords = Collections.emptyList();
        assertDoesNotThrow(() -> tableWriter.insertRecords(emptyRecords));

        verify(mockIngestionService, never()).addRecords(any());
    }

    @Test
    void shouldThrowSQLExceptionWhenIngestionFails() throws SQLException {
        doThrow(SQLException.class).when(mockIngestionService).addRecords(anyList());
        assertThrows(SQLException.class, () -> tableWriter.insertRecords(List.of(buildRecord(0, 100L))));
        // Exactly-once invariant: offsets must NOT be persisted when ingestion fails —
        // persisting them would cause records to be skipped on restart even though they were never written.
        verify(mockFireboltMetadataService, never()).updateOffsets(any(), any());
    }


    @Test
    void shouldCloseTableWriterSuccessfully() throws Exception {
        doNothing().when(mockIngestionService).close();
        tableWriter.close();
        verify(mockIngestionService).close();
    }

    @Test
    void shouldHandleIngestionServiceCloseException() throws Exception {
        doThrow(Exception.class).when(mockIngestionService).close();
        tableWriter.close();
        verify(mockIngestionService).close();
    }

    @Test
    void shouldHandleMultipleCloseCallsSafely() throws Exception {
        doNothing().when(mockIngestionService).close();
        tableWriter.close();
        tableWriter.close();
        verify(mockIngestionService, times(2)).close();
    }

    @Test
    void shouldPersistOffsetsViaMetadataServiceAfterSuccessfulBatch() throws SQLException {
        // Exactly-once: after inserting records, updateOffsets must be called so restarts
        // don't reprocess already-committed records.
        List<SinkRecord> records = List.of(
                buildRecord(0, 42L),
                buildRecord(1, 99L));
        doNothing().when(mockIngestionService).addRecords(records);

        tableWriter.insertRecords(records);

        // Offset map must be persisted after the batch, not just updated in memory.
        verify(mockFireboltMetadataService).updateOffsets(TOPIC_NAME, tableWriter.getProcessedPartitionOffsets());
    }

    @Test
    void shouldNotAdvanceLocalOffsetsWhenPersistenceFails() throws SQLException {
        // Persist-before-local-update invariant: if the DB write throws, the in-memory offset
        // map must stay at its old value so the next batch retries persisting those offsets.
        doThrow(new RuntimeException("DB unavailable"))
                .when(mockFireboltMetadataService).updateOffsets(any(), any());

        assertThrows(RuntimeException.class,
                () -> tableWriter.insertRecords(List.of(buildRecord(0, 50L))));

        // Local state must not have advanced — offset stays at -1, not 50.
        assertEquals(-1L, tableWriter.getProcessedPartitionOffsets().get(PARTITION_0));
    }

    @Test
    void shouldNotCallUpdateOffsetsWhenMetadataServiceIsNull() throws SQLException {
        // At-least-once mode: no metadata service → no persistence call, no NPE.
        TableWriter writerWithoutMetadata = new TableWriter(
                mockTableSchema, null, TOPIC_NAME,
                new HashMap<>(Map.of(0, -1L)), mockIngestionService);

        doNothing().when(mockIngestionService).addRecords(anyList());

        assertDoesNotThrow(() -> writerWithoutMetadata.insertRecords(List.of(buildRecord(0, 10L))));
    }

    private static SinkRecord buildRecord(int partition, long offset) {
        // The 7-arg constructor makes originalKafkaPartition()/originalKafkaOffset()
        // return the given partition/offset.
        return new SinkRecord(TOPIC_NAME, partition, null, null, null, null, offset);
    }

}
