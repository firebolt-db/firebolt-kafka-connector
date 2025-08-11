package com.firebolt.kafka.connect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableWriterTest {

    private static final String TABLE_NAME = "test_table";

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
    private Supplier<Connection> mockConnectionSupplier;
    
    @Mock
    private Connection mockConnection;
    
    @Mock
    private TableWriter.InsertPreparedStatementProvider mockInsertPrepareStatementProvider;

    @Mock
    private InsertPreparedStatement mockInsertPrepareStatement;

    private TableWriter tableWriter;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        when(mockTableSchema.getTableName()).thenReturn(TABLE_NAME);
        when(mockTableSchema.getColumns()).thenReturn(List.of(mockColumn1, mockColumn2));

        // Setup default mock behaviors
        when(mockConnectionSupplier.get()).thenReturn(mockConnection);
        // by default connection is not closed
        when(mockConnection.isClosed()).thenReturn(false);

        tableWriter = new TableWriter(mockTableSchema, mockConnectionSupplier, new HashMap<>(), mockInsertPrepareStatementProvider);
        when(mockInsertPrepareStatementProvider.get(mockConnection, mockTableSchema)).thenReturn(mockInsertPrepareStatement);

        doNothing().when(mockInsertPrepareStatement).addRecords(anyList());
    }

    @Test
    void shouldInitializeWithEmptyProcessedPartitionOffsets() {
        assertNotNull(tableWriter.getProcessedPartitionOffsets());
        assertTrue(tableWriter.getProcessedPartitionOffsets().isEmpty());

        // should not be able to modify the offsets outside the class
        assertThrows(RuntimeException.class, () -> tableWriter.getProcessedPartitionOffsets().put(1, 1L));
    }

    @Test
    void shouldInsertRecordsSuccessfully() throws SQLException {
        when(mockFireboltRecord1.getPartition()).thenReturn(0);
        when(mockFireboltRecord1.getOffset()).thenReturn(100L);

        when(mockFireboltRecord2.getPartition()).thenReturn(1);
        when(mockFireboltRecord2.getOffset()).thenReturn(200L);

        when(mockFireboltRecord3.getPartition()).thenReturn(2);
        when(mockFireboltRecord3.getOffset()).thenReturn(300L);

        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1, mockFireboltRecord2, mockFireboltRecord3)));
        verify(mockConnectionSupplier).get();

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

        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1, mockFireboltRecord2, mockFireboltRecord3)));
        verify(mockConnectionSupplier).get();

        Map<Integer, Long> offsets = tableWriter.getProcessedPartitionOffsets();
        assertEquals(1, offsets.size());
        assertEquals(102L, offsets.get(0));
    }


    @Test
    void shouldHandleEmptyRecordsList() {
        List<FireboltRecord> emptyRecords = Collections.emptyList();
        assertDoesNotThrow(() -> tableWriter.insertRecords(emptyRecords));

        verify(mockInsertPrepareStatementProvider, never()).get(any(), any());
        verify(mockConnectionSupplier, never()).get();
    }

    @Test
    void shouldThrowSQLExceptionWhenInsertFails() throws SQLException {
        doThrow(SQLException.class).when(mockInsertPrepareStatement).addRecords(anyList());
        assertThrows(SQLException.class, () -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));
    }

    @Test
    void shouldCreateNewConnectionWhenConnectionIsNull() throws SQLException {
        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));
        verify(mockConnectionSupplier).get();
    }

    @Test
    void shouldCreateNewConnectionWhenConnectionIsClosed() throws SQLException {
        Connection mockSecondConnection = mock(Connection.class);
        when(mockConnectionSupplier.get()).thenReturn(mockConnection, mockSecondConnection);

        InsertPreparedStatement mockSecondInsertPreparedStatement = mock(InsertPreparedStatement.class);
        when(mockInsertPrepareStatementProvider.get(mockSecondConnection, mockTableSchema)).thenReturn(mockSecondInsertPreparedStatement);

        // do the first insert on the first connection
        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));
        verify(mockInsertPrepareStatement).addRecords(List.of(mockFireboltRecord1));

        // first connection was closed
        when(mockConnection.isClosed()).thenReturn(true);
        when(mockSecondConnection.isClosed()).thenReturn(false);

        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1)));
        verify(mockConnectionSupplier, times(2)).get();
        verify(mockSecondInsertPreparedStatement).addRecords(List.of(mockFireboltRecord1));
    }

    @Test
    void shouldCloseConnectionSuccessfully() throws SQLException {
        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1))); // This creates the connection
        tableWriter.close();
        verify(mockConnection).close();
    }

    @Test
    void shouldHandleNullConnectionInClose() throws SQLException {
        assertDoesNotThrow(() -> tableWriter.close());
        verify(mockConnection, never()).close();
    }

    @Test
    void shouldHandleConnectionCloseException() throws SQLException {
        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1))); // This creates the connection
        doThrow(new SQLException("Connection close failed")).when(mockConnection).close();
        assertDoesNotThrow(() -> tableWriter.close());
        verify(mockConnection).close();
    }

    @Test
    void shouldHandleMultipleCloseCallsSafely() throws SQLException {
        when(mockConnection.isClosed()).thenReturn(false);
        assertDoesNotThrow(() -> tableWriter.insertRecords(List.of(mockFireboltRecord1))); // This creates the connection
        tableWriter.close();
        tableWriter.close();
        verify(mockConnection, times(2)).close();
    }

}
