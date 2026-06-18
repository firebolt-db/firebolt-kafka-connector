package com.firebolt.kafka.connect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestionServiceWithPostProcessingTest {

    private static final String FIXED_BATCH_ID = "11111111-1111-1111-1111-111111111111";

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private IngestionService mockIngestionService;

    @Captor
    private ArgumentCaptor<String> postProcessingScriptCaptor;

    @Captor
    private ArgumentCaptor<List<SinkRecord>> sinkRecordListCaptor;

    @Captor
    private ArgumentCaptor<Map<String, String>> literalColumnsCaptor;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.execute(anyString())).thenReturn(true);
    }

    @Test
    void shouldPassBatchIdAsLiteralColumnAndExecutePostProcessingScript() throws Exception {
        String postProcessingScript = "DELETE FROM some_tmp WHERE batch_id='${firebolt_param.batch_id}'";

        IngestionServiceWithPostProcessing ingestionServiceWithPostProcessing = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, postProcessingScript
        );

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 1L);

        List<SinkRecord> records = List.of(record);
        assertDoesNotThrow(() -> ingestionServiceWithPostProcessing.addRecords(records));

        verify(mockConnection).setAutoCommit(false);

        // The delegate must receive the same records plus a generated batch id as a literal column
        verify(mockIngestionService).addRecords(sinkRecordListCaptor.capture(), literalColumnsCaptor.capture());
        assertEquals(records, sinkRecordListCaptor.getValue());

        Map<String, String> literalColumns = literalColumnsCaptor.getValue();
        String batchId = literalColumns.get(BATCH_ID_COLUMN_NAME);
        assertNotNull(batchId);

        verify(mockConnection).createStatement();

        // The same batch id must be substituted into the post-processing script
        verify(mockStatement).execute(postProcessingScriptCaptor.capture());
        assertEquals("DELETE FROM some_tmp WHERE batch_id='" + batchId + "'", postProcessingScriptCaptor.getValue());

        verify(mockConnection, times(1)).commit();
    }

    @Test
    void shouldPreserveCallerLiteralColumnsWhenAddingBatchId() throws Exception {
        IngestionServiceWithPostProcessing subject = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, "select 1"
        );

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 1L);
        assertDoesNotThrow(() -> subject.addRecords(List.of(record), Map.of("source", "kafka")));

        verify(mockIngestionService).addRecords(anyList(), literalColumnsCaptor.capture());
        Map<String, String> literalColumns = literalColumnsCaptor.getValue();
        assertEquals("kafka", literalColumns.get("source"));
        assertTrue(literalColumns.containsKey(BATCH_ID_COLUMN_NAME));
    }

    @Test
    void shouldRollbackAndRethrowOnFailure() throws Exception {
        String postProcessingScript = "UPDATE x SET y=1 WHERE batch_id='${firebolt_param.batch_id}'";

        doThrow(new SQLException("boom")).when(mockIngestionService).addRecords(anyList(), anyMap());

        IngestionServiceWithPostProcessing ingestionServiceWithPostProcessing = new IngestionServiceWithPostProcessing(
                mockIngestionService, mockConnection, postProcessingScript
        );

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 1L);

        assertThrows(SQLException.class, () -> ingestionServiceWithPostProcessing.addRecords(List.of(record)));

        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).rollback();
        verify(mockConnection).commit();
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

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 1L);

        assertThrows(SQLException.class, () -> subject.addRecords(List.of(record)));

        verify(mockConnection, times(1)).setAutoCommit(false);
        // script was attempted before failing commit
        verify(mockStatement, times(1)).execute(Mockito.argThat(sql -> sql.startsWith("DELETE FROM some_tmp")));
        verify(mockConnection, times(1)).rollback();
        verify(mockConnection, times(1)).commit();
    }
}
