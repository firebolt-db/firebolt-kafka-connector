package com.firebolt.kafka.connect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.firebolt.kafka.connect.IngestionServiceWithPostProcessing.BATCH_ID_COLUMN_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestionServiceWithPostProcessingTest {

    private static final String TABLE_NAME = "post_process_table";
    private static final String FIXED_BATCH_ID = "11111111-1111-1111-1111-111111111111";

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private Statement mockStatement;

    @Mock
    private IngestionService mockIngestionService;

    private TableSchema tableSchema;

    @Captor
    private ArgumentCaptor<String> postProcessingScriptCaptor;

    @Captor
    private ArgumentCaptor<List<AbstractFireboltRecord>> fireboltRecordListCaptor;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        tableSchema = new TableSchema(TABLE_NAME);
        tableSchema.addColumn("batch_id", "text", Types.VARCHAR, true);

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1});
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.execute(anyString())).thenReturn(true);
    }

    @Test
    void shouldInsertWithBatchIdAndExecutePostProcessingScript() throws Exception {
        String postProcessingScript = "DELETE FROM some_tmp WHERE batch_id='${firebolt_param.batch_id}'";

        IngestionServiceWithPostProcessing ingestionServiceWithPostProcessing = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, postProcessingScript
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        List<AbstractFireboltRecord> records = List.of(record);
        assertDoesNotThrow(() -> ingestionServiceWithPostProcessing.addRecords(records));

        verify(mockConnection).setAutoCommit(false);
        verify(mockIngestionService).addRecords(fireboltRecordListCaptor.capture());
        List<AbstractFireboltRecord> fireboltRecords = fireboltRecordListCaptor.getValue();
        assertEquals(1, fireboltRecords.size());

        AbstractFireboltRecord fireboltRecord = fireboltRecords.get(0);
        Object batchId = fireboltRecord.getColumnValue(BATCH_ID_COLUMN_NAME).getValue();
        assertNotNull(batchId);

        verify(mockConnection).createStatement();

        verify(mockStatement).execute(postProcessingScriptCaptor.capture());
        assertEquals("DELETE FROM some_tmp WHERE batch_id='"+batchId+"'", postProcessingScriptCaptor.getValue());

        verify(mockConnection, times(1)).commit();
    }

    @Test
    void shouldRollbackAndRethrowOnFailure() throws Exception {
        String postProcessingScript = "UPDATE x SET y=1 WHERE batch_id='${firebolt_param.batch_id}'";

        doThrow(new SQLException("boom")).when(mockIngestionService).addRecords(any());

        IngestionServiceWithPostProcessing ingestionServiceWithPostProcessing = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, postProcessingScript
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        assertThrows(SQLException.class, () -> ingestionServiceWithPostProcessing.addRecords(List.of(record)));

        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).rollback();
        // commit must NOT be called after a rollback — the original code had a bug here
        verify(mockConnection, times(0)).commit();
        verify(mockConnection).setAutoCommit(true);
        Mockito.verifyNoInteractions(mockStatement);
    }

    @Test
    void processScriptShouldReplaceBatchId() {
        IngestionServiceWithPostProcessing subject = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, ""
        );
        String script = "select '${firebolt_param.batch_id}'";
        String processed = subject.processScript(script, FIXED_BATCH_ID);
        assertEquals("select '" + FIXED_BATCH_ID + "'", processed);
    }

    @Test
    void processScriptShouldReturnAsIsForNullOrBlank() {
        IngestionServiceWithPostProcessing subject = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, ""
        );
        assertEquals(null, subject.processScript(null, FIXED_BATCH_ID));
        assertEquals("   ", subject.processScript("   ", FIXED_BATCH_ID));
    }

    @Test
    void willRollBackTransactionIfCommitFails() throws Exception {
        String postProcessingScript = "DELETE FROM some_tmp WHERE batch_id='${firebolt_param.batch_id}'";

        Mockito.doThrow(new SQLException("commit failure")).when(mockConnection).commit();

        IngestionServiceWithPostProcessing subject = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, postProcessingScript
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        assertThrows(SQLException.class, () -> subject.addRecords(List.of(record)));

        verify(mockConnection, times(1)).setAutoCommit(false);
        // script was attempted before failing commit
        verify(mockStatement, times(1)).execute(Mockito.argThat(sql -> sql.startsWith("DELETE FROM some_tmp")));
        verify(mockConnection, times(1)).rollback();
        verify(mockConnection, times(1)).commit();
        verify(mockConnection, times(1)).setAutoCommit(true);
    }

    @Test
    void shouldNotManageTransactionWhenFlagIsFalse() throws Exception {
        // In exactly-once mode the transaction is owned by TableWriter, so this decorator
        // must not call setAutoCommit / commit / rollback.
        String postProcessingScript = "DELETE FROM some_tmp WHERE batch_id='${firebolt_param.batch_id}'";

        IngestionServiceWithPostProcessing subject = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, postProcessingScript, false
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        assertDoesNotThrow(() -> subject.addRecords(List.of(record)));

        verify(mockIngestionService).addRecords(fireboltRecordListCaptor.capture());
        verify(mockConnection).createStatement();
        verify(mockStatement).execute(Mockito.argThat(sql -> sql.startsWith("DELETE FROM some_tmp")));

        // transaction management is the caller's responsibility
        verify(mockConnection, times(0)).setAutoCommit(false);
        verify(mockConnection, times(0)).commit();
        verify(mockConnection, times(0)).rollback();
    }
}


