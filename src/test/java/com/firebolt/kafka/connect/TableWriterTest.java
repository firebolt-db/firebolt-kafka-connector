package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltMetadataService;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private TableSchema.Column mockColumn1;

    @Mock
    private TableSchema.Column mockColumn2;

    @Mock
    private FireboltRecord mockFireboltRecord1;

    @Mock
    private FireboltRecord mockFireboltRecord2;

    @Mock
    private FireboltRecord mockFireboltRecord3;

    @Mock
    private IngestionService mockIngestionService;

    @Mock
    private FireboltMetadataService mockFireboltMetadataService;

    private TableWriter tableWriter;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        when(mockTableSchema.getTableName()).thenReturn(TABLE_NAME);
        when(mockTableSchema.getColumns()).thenReturn(List.of(mockColumn1, mockColumn2));

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
        when(mockFireboltRecord1.getPartition()).thenReturn(0);
        when(mockFireboltRecord1.getOffset()).thenReturn(100L);

        when(mockFireboltRecord2.getPartition()).thenReturn(1);
        when(mockFireboltRecord2.getOffset()).thenReturn(200L);

        when(mockFireboltRecord3.getPartition()).thenReturn(2);
        when(mockFireboltRecord3.getOffset()).thenReturn(300L);

        List<AbstractFireboltRecord> records = List.of(mockFireboltRecord1, mockFireboltRecord2, mockFireboltRecord3);
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
        when(mockFireboltRecord1.getPartition()).thenReturn(0);
        when(mockFireboltRecord1.getOffset()).thenReturn(100L);

        when(mockFireboltRecord2.getPartition()).thenReturn(0);
        when(mockFireboltRecord2.getOffset()).thenReturn(101L);

        when(mockFireboltRecord3.getPartition()).thenReturn(0);
        when(mockFireboltRecord3.getOffset()).thenReturn(102L);

        List<AbstractFireboltRecord> records = List.of(mockFireboltRecord1, mockFireboltRecord2, mockFireboltRecord3);
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
        List<AbstractFireboltRecord> emptyRecords = Collections.emptyList();
        assertDoesNotThrow(() -> tableWriter.insertRecords(emptyRecords));

        verify(mockIngestionService, never()).addRecords(any());
    }

    @Test
    void shouldThrowSQLExceptionWhenIngestionFails() throws SQLException {
        doThrow(SQLException.class).when(mockIngestionService).addRecords(anyList());
        assertThrows(SQLException.class, () -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));
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

}
