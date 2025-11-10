package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InsertPreparedStatementWithPostProcessingTest {

    private static final String TABLE_NAME = "post_process_table";
    private static final String FIXED_BATCH_ID = "11111111-1111-1111-1111-111111111111";

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Statement statement;

    @Mock
    private ErrorReporter errorReporter;

    private TableSchema tableSchema;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        tableSchema = new TableSchema(TABLE_NAME);
        tableSchema.addColumn("batch_id", "text", Types.VARCHAR, true);

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeBatch()).thenReturn(new int[] {1});
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
    }

    @Test
    void shouldInsertWithBatchIdAndExecutePostProcessingScript() throws Exception {
        String postProcessingScript = "DELETE FROM some_tmp WHERE batch_id='${firebolt_param.batch_id}'";

        InsertPreparedStatementWithPostProcessing subject = new InsertPreparedStatementWithPostProcessing(
                connection, tableSchema, errorReporter, false, postProcessingScript
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        assertDoesNotThrow(() -> subject.addRecords(List.of(record)));

        verify(connection, times(1)).setAutoCommit(false);
        verify(connection, times(1)).prepareStatement(Mockito.argThat(sql -> sql.contains("\"batch_id\"")));
        verify(preparedStatement, times(1)).executeBatch();

        ArgumentCaptor<String> scriptCaptor = ArgumentCaptor.forClass(String.class);
        verify(statement, times(1)).execute(scriptCaptor.capture());
        String executed = scriptCaptor.getValue();
        Pattern p = Pattern.compile("DELETE FROM some_tmp WHERE batch_id='[0-9a-fA-F-]{36}'");
        org.junit.jupiter.api.Assertions.assertTrue(p.matcher(executed).matches());

        verify(connection, times(1)).commit();
    }

    @Test
    void shouldRollbackAndRethrowOnFailure() throws Exception {
        String postProcessingScript = "UPDATE x SET y=1 WHERE batch_id='${firebolt_param.batch_id}'";

        when(preparedStatement.executeBatch()).thenThrow(new SQLException("boom"));

        InsertPreparedStatementWithPostProcessing subject = new InsertPreparedStatementWithPostProcessing(
                connection, tableSchema, errorReporter, false, postProcessingScript
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        assertThrows(SQLException.class, () -> subject.addRecords(List.of(record)));

        verify(connection, times(1)).setAutoCommit(false);
        verify(connection, times(1)).rollback();
        verify(connection, times(1)).commit();
        Mockito.verifyNoInteractions(statement);
    }

    @Test
    void processScriptShouldReplaceBatchId() {
        InsertPreparedStatementWithPostProcessing subject = new InsertPreparedStatementWithPostProcessing(
                connection, tableSchema, errorReporter, false, ""
        );
        String script = "select '${firebolt_param.batch_id}'";
        String processed = subject.processScript(script, FIXED_BATCH_ID);
        assertEquals("select '" + FIXED_BATCH_ID + "'", processed);
    }

    @Test
    void processScriptShouldReturnAsIsForNullOrBlank() {
        InsertPreparedStatementWithPostProcessing subject = new InsertPreparedStatementWithPostProcessing(
                connection, tableSchema, errorReporter, false, ""
        );
        assertEquals(null, subject.processScript(null, FIXED_BATCH_ID));
        assertEquals("   ", subject.processScript("   ", FIXED_BATCH_ID));
    }

    @Test
    void willRollBackTransactionIfCommitFails() throws Exception {
        String postProcessingScript = "DELETE FROM some_tmp WHERE batch_id='${firebolt_param.batch_id}'";

        Mockito.doThrow(new SQLException("commit failure")).when(connection).commit();

        InsertPreparedStatementWithPostProcessing subject = new InsertPreparedStatementWithPostProcessing(
                connection, tableSchema, errorReporter, false, postProcessingScript
        );

        FireboltRecord record = new FireboltRecord(
                TABLE_NAME,
                Collections.emptyMap(),
                new SinkRecord("topic", 0, null, null, null, null, 1L)
        );

        assertThrows(SQLException.class, () -> subject.addRecords(List.of(record)));

        verify(connection, times(1)).setAutoCommit(false);
        // script was attempted before failing commit
        verify(statement, times(1)).execute(Mockito.argThat(sql -> sql.startsWith("DELETE FROM some_tmp")));
        verify(connection, times(1)).rollback();
        verify(connection, times(1)).commit();
    }
}


