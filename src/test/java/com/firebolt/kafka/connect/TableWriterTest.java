package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltMetadataService;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
    private Connection mockConnection;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        when(mockTableSchema.getTableName()).thenReturn(TABLE_NAME);
        when(mockTableSchema.getColumns()).thenReturn(List.of(mockColumn1, mockColumn2));

        doNothing().when(mockIngestionService).addRecords(anyList());
    }

    private Map<Integer, Long> defaultOffsets() {
        Map<Integer, Long> offsets = new HashMap<>();
        offsets.put(PARTITION_0, -1L);
        offsets.put(PARTITION_1, -1L);
        offsets.put(PARTITION_2, -1L);
        return offsets;
    }

    /** At-least-once mode: no metadata service, no explicit transaction management. */
    @Nested
    class AtLeastOnce {

        private TableWriter tableWriter;

        @BeforeEach
        void setUp() {
            tableWriter = new TableWriter(mockTableSchema, mockConnection, null, TOPIC_NAME, defaultOffsets(), mockIngestionService);
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
            assertDoesNotThrow(() -> tableWriter.insertRecords(records));

            Map<Integer, Long> offsets = tableWriter.getProcessedPartitionOffsets();
            assertEquals(3, offsets.size());
            assertEquals(100L, offsets.get(0));
            assertEquals(200L, offsets.get(1));
            assertEquals(300L, offsets.get(2));

            // no transaction management in at-least-once mode
            verify(mockConnection, never()).setAutoCommit(false);
            verify(mockConnection, never()).commit();
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
            assertDoesNotThrow(() -> tableWriter.insertRecords(records));

            Map<Integer, Long> offsets = tableWriter.getProcessedPartitionOffsets();
            assertEquals(3, offsets.size());
            assertEquals(102L, offsets.get(0));
            assertEquals(-1L, offsets.get(1));
            assertEquals(-1L, offsets.get(2));
        }

        @Test
        void shouldHandleEmptyRecordsList() throws Exception {
            assertDoesNotThrow(() -> tableWriter.insertRecords(Collections.emptyList()));
            verify(mockIngestionService, never()).addRecords(any());
        }

        @Test
        void shouldThrowSQLExceptionWhenIngestionFails() throws SQLException {
            doThrow(SQLException.class).when(mockIngestionService).addRecords(anyList());
            assertThrows(SQLException.class, () -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));
            // no transaction to roll back in at-least-once mode
            verify(mockConnection, never()).rollback();
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

    /** Exactly-once mode: wraps each batch in BEGIN; INSERT data; UPDATE offsets; COMMIT. */
    @Nested
    class ExactlyOnce {

        @Mock
        private FireboltMetadataService mockMetadataService;

        private TableWriter tableWriter;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.openMocks(this);
            tableWriter = new TableWriter(mockTableSchema, mockConnection, mockMetadataService, TOPIC_NAME, defaultOffsets(), mockIngestionService);
        }

        @Test
        void shouldWrapInsertAndOffsetUpdateInSingleTransaction() throws SQLException {
            when(mockFireboltRecord1.getPartition()).thenReturn(0);
            when(mockFireboltRecord1.getOffset()).thenReturn(100L);

            when(mockFireboltRecord2.getPartition()).thenReturn(1);
            when(mockFireboltRecord2.getOffset()).thenReturn(200L);

            List<AbstractFireboltRecord> records = List.of(mockFireboltRecord1, mockFireboltRecord2);
            assertDoesNotThrow(() -> tableWriter.insertRecords(records));

            // transaction lifecycle
            verify(mockConnection).setAutoCommit(false);
            verify(mockConnection).commit();
            verify(mockConnection, never()).rollback();
            verify(mockConnection).setAutoCommit(true);

            // data insert happened
            verify(mockIngestionService).addRecords(records);

            // offset update happened on the same connection, inside the transaction
            verify(mockMetadataService).updateOffsets(eq(mockConnection), eq(TOPIC_NAME), eq(Map.of(0, 100L, 1, 200L)));

            // in-memory offsets updated after commit
            assertEquals(100L, tableWriter.getProcessedPartitionOffsets().get(0));
            assertEquals(200L, tableWriter.getProcessedPartitionOffsets().get(1));
        }

        @Test
        void shouldRollbackAndRethrowWhenInsertFails() throws SQLException {
            doThrow(new SQLException("insert failure")).when(mockIngestionService).addRecords(anyList());
            when(mockFireboltRecord1.getPartition()).thenReturn(0);

            assertThrows(SQLException.class, () -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));

            verify(mockConnection).setAutoCommit(false);
            verify(mockConnection).rollback();
            verify(mockConnection, never()).commit();
            verify(mockConnection).setAutoCommit(true);

            // offset metadata must NOT be updated after a rollback
            verify(mockMetadataService, never()).updateOffsets(any(), anyString(), anyMap());
        }

        @Test
        void shouldRollbackAndRethrowWhenOffsetUpdateFails() throws SQLException {
            when(mockFireboltRecord1.getPartition()).thenReturn(0);
            when(mockFireboltRecord1.getOffset()).thenReturn(50L);
            doThrow(new SQLException("update failure")).when(mockMetadataService).updateOffsets(any(), anyString(), anyMap());

            assertThrows(SQLException.class, () -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));

            verify(mockConnection).setAutoCommit(false);
            verify(mockConnection).rollback();
            verify(mockConnection, never()).commit();
            verify(mockConnection).setAutoCommit(true);
        }

        @Test
        void shouldSkipTransactionForEmptyRecords() throws Exception {
            assertDoesNotThrow(() -> tableWriter.insertRecords(Collections.emptyList()));
            verify(mockConnection, never()).setAutoCommit(false);
            verify(mockMetadataService, never()).updateOffsets(any(), anyString(), anyMap());
        }

        @Test
        void offsetUpdateShouldUseMaxOffsetPerPartition() throws SQLException {
            // Three records on the same partition — only the highest offset should be committed
            when(mockFireboltRecord1.getPartition()).thenReturn(0);
            when(mockFireboltRecord1.getOffset()).thenReturn(10L);
            when(mockFireboltRecord2.getPartition()).thenReturn(0);
            when(mockFireboltRecord2.getOffset()).thenReturn(12L);
            when(mockFireboltRecord3.getPartition()).thenReturn(0);
            when(mockFireboltRecord3.getOffset()).thenReturn(11L);

            List<AbstractFireboltRecord> records = List.of(mockFireboltRecord1, mockFireboltRecord2, mockFireboltRecord3);
            assertDoesNotThrow(() -> tableWriter.insertRecords(records));

            verify(mockMetadataService).updateOffsets(eq(mockConnection), eq(TOPIC_NAME), eq(Map.of(0, 12L)));
        }
    }
}
