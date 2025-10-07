package com.firebolt.kafka.connect;

import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeColumnFactoryProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Mockito;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverterFactory;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InsertPreparedStatementTest {

    private static final String TABLE_NAME = "test_table";
    private static final String COLUMN_NAME_1 = "id";
    private static final String COLUMN_NAME_2 = "NAME";

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private TableSchema mockTableSchema;

    @Mock
    private TableSchema.Column mockColumn1;

    @Mock
    private TableSchema.Column mockColumn2;

    private InsertPreparedStatement insertPreparedStatement;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        when(mockConnection.prepareStatement(argThat(sqlContains("INSERT INTO")))).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

        when(mockColumn1.getName()).thenReturn(COLUMN_NAME_1);
        when(mockColumn1.getDataType()).thenReturn("integer");
        when(mockColumn1.getSqlType()).thenReturn(Types.INTEGER);
        when(mockColumn2.getName()).thenReturn(COLUMN_NAME_2);
        when(mockColumn2.getDataType()).thenReturn("text");
        when(mockColumn2.getSqlType()).thenReturn(Types.VARCHAR);
        
        when(mockTableSchema.getTableName()).thenReturn(TABLE_NAME);
        when(mockTableSchema.getColumns()).thenReturn(List.of(mockColumn1, mockColumn2));
        doNothing().when(errorReporter).report(any(), any());

        insertPreparedStatement = new InsertPreparedStatement(mockConnection, mockTableSchema, errorReporter, false);
    }

    @Test
    void shouldSplitBatchOnHttp413AndInsertHalves() throws Exception {
        // 4 records so we expect first executeBatch to fail with 413, then two successful halves
        List<FireboltRecord> records = new ArrayList<>();
        records.add(buildRecord(TABLE_NAME, 0, 1L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(1).schemaType(Schema.Type.INT32).build(),
                "NAME", KafkaMessageColumnValue.builder().value("a").schemaType(Schema.Type.STRING).build()
        )));
        records.add(buildRecord(TABLE_NAME, 0, 2L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(2).schemaType(Schema.Type.INT32).build(),
                "NAME", KafkaMessageColumnValue.builder().value("b").schemaType(Schema.Type.STRING).build()
        )));
        records.add(buildRecord(TABLE_NAME, 0, 3L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(3).schemaType(Schema.Type.INT32).build(),
                "NAME", KafkaMessageColumnValue.builder().value("c").schemaType(Schema.Type.STRING).build()
        )));
        records.add(buildRecord(TABLE_NAME, 0, 4L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(4).schemaType(Schema.Type.INT32).build(),
                "NAME", KafkaMessageColumnValue.builder().value("d").schemaType(Schema.Type.STRING).build()
        )));

        // Prepare three statements: first fails with 413, next two succeed
        PreparedStatement psFail = Mockito.mock(PreparedStatement.class);
        PreparedStatement psLeft = Mockito.mock(PreparedStatement.class);
        PreparedStatement psRight = Mockito.mock(PreparedStatement.class);

        // FireboltException simulating HTTP 413 (payload too large)
        FireboltException http413 = Mockito.mock(FireboltException.class);
        when(http413.getType()).thenReturn(ExceptionType.REQUEST_BODY_TOO_LARGE);

        when(psFail.executeBatch()).thenThrow(http413);
        when(psLeft.executeBatch()).thenReturn(new int[] {1, 1});
        when(psRight.executeBatch()).thenReturn(new int[] {1, 1});

        // Return statements in sequence for three createPreparedStatement() calls
        when(mockConnection.prepareStatement(anyString())).thenReturn(psFail, psLeft, psRight);

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        // Verify executeBatch called once on each PS (1 fail + 2 successes)
        verify(psFail, times(1)).executeBatch();
        verify(psLeft, times(1)).executeBatch();
        verify(psRight, times(1)).executeBatch();
    }

    @Test
    void shouldReportSingleRecordTooLargeViaErrorReporter() throws Exception {
        // Use a new InsertPreparedStatement with error tolerance enabled
        insertPreparedStatement = new InsertPreparedStatement(mockConnection, mockTableSchema, errorReporter, true);

        List<FireboltRecord> records = new ArrayList<>();
        records.add(buildRecord(TABLE_NAME, 1, 1234L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(5L).schemaType(Schema.Type.INT64).build(),
                "NAME", KafkaMessageColumnValue.builder().value("huge").schemaType(Schema.Type.STRING).build()
        )));

        PreparedStatement psFail = Mockito.mock(PreparedStatement.class);
        FireboltException http413 = Mockito.mock(FireboltException.class);
        when(http413.getType()).thenReturn(ExceptionType.REQUEST_BODY_TOO_LARGE);
        when(psFail.executeBatch()).thenThrow(http413);
        when(mockConnection.prepareStatement(anyString())).thenReturn(psFail);

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        verify(errorReporter, times(1)).report(
                Mockito.argThat(rec -> rec.topic().equals("topic") && rec.kafkaOffset() == 1234L),
                Mockito.any(FireboltException.class)
        );
    }

//    @Test
//    void willPropagateColumnConversionFailureAsRecordConversionFailedException() throws Exception {
//        // Arrange a record with values that will trigger a converter failure
//        FireboltRecord record = buildRecord(TABLE_NAME, 7, 123L, mapOf(
//                "id", KafkaMessageColumnValue.builder().value(1L).schemaType(Schema.Type.INT64).build(),
//                "NAME", KafkaMessageColumnValue.builder().value("not_a_boolean").schemaType(Schema.Type.STRING).build()
//        ));
//
//        // Mock converter to throw ColumnConversionFailedException
//        ColumnDataTypeConverter mockConverter = mock(ColumnDataTypeConverter.class);
//        Mockito.doThrow(new ColumnConversionFailedException(COLUMN_NAME_2, "text", "boom"))
//                .when(mockConverter).convertAndSet(Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any());
//
//        ColumnDataTypeConverterFactory mockFactory = mock(ColumnDataTypeConverterFactory.class);
//        Mockito.when(mockFactory.getConverter(Mockito.any())).thenReturn(mockConverter);
//
//        // Valid column names map (firebolt column name -> record attribute name)
//        Map<String, String> validColumnNames = new java.util.HashMap<>();
//        validColumnNames.put(COLUMN_NAME_1, "id");
//        validColumnNames.put(COLUMN_NAME_2, "NAME");
//
//        try (MockedStatic<ColumnDataTypeColumnFactoryProvider> factoryStatic = Mockito.mockStatic(ColumnDataTypeColumnFactoryProvider.class)) {
//            factoryStatic.when(ColumnDataTypeColumnFactoryProvider.getInstance(false)).thenReturn(mockFactory);
//
//            // Use reflection to invoke private setStatementParameters and verify it throws RecordConversionFailedException
//            java.lang.reflect.Method method = InsertPreparedStatement.class.getDeclaredMethod(
//                    "setStatementParameters", PreparedStatement.class, FireboltRecord.class, TableSchema.class, Map.class);
//            method.setAccessible(true);
//
//            RecordConversionFailedException thrown = assertThrows(
//                    RecordConversionFailedException.class,
//                    () -> {
//                        try {
//                            method.invoke(insertPreparedStatement, mockPreparedStatement, record, mockTableSchema, validColumnNames);
//                        } catch (java.lang.reflect.InvocationTargetException ite) {
//                            // unwrap
//                            Throwable cause = ite.getCause();
//                            if (cause instanceof RuntimeException) {
//                                throw (RuntimeException) cause;
//                            }
//                            if (cause instanceof Error) {
//                                throw (Error) cause;
//                            }
//                            throw new RuntimeException(cause);
//                        }
//                    }
//            );
//
//            // Assert details propagated to record-level exception
//            assertEquals(TABLE_NAME, thrown.getTableName());
//            assertEquals("topic", thrown.getTopicName());
//            assertEquals(7, thrown.getKafkaPartition());
//            assertEquals(123L, thrown.getKafkaOffset());
//        }
//    }

    @Test
    void shouldBuildCorrectInsertSQLAndSetParametersWithCaseInsensitiveColumnNames() throws SQLException {        
        List<FireboltRecord> records = new ArrayList<>();
        records.add(buildRecord(TABLE_NAME, 0, 10L, mapOf(
                "ID", KafkaMessageColumnValue.builder().value(Integer.valueOf(123)).schemaType(Schema.Type.INT32).build(),
                "name", KafkaMessageColumnValue.builder().value("Alice").schemaType(Schema.Type.STRING).build()
        )));
        records.add(buildRecord(TABLE_NAME, 0, 11L, mapOf(
                "ID", KafkaMessageColumnValue.builder().value(Integer.valueOf(456)).schemaType(Schema.Type.INT32).build(),
                "name", KafkaMessageColumnValue.builder().value("Bob").schemaType(Schema.Type.STRING).build()
        )));

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        verify(mockConnection).prepareStatement(argThat(sqlContains("INSERT INTO \"test_table\" (\"id\", \"NAME\") VALUES (?, ?)")));

        verify(mockPreparedStatement).setInt(1, 123);
        verify(mockPreparedStatement).setString(2, "Alice");
        verify(mockPreparedStatement).setInt(1, 456);
        verify(mockPreparedStatement).setString(2, "Bob");
        verify(mockPreparedStatement, times(2)).addBatch();
        verify(mockPreparedStatement).executeBatch();
        verify(mockPreparedStatement).clearBatch();
        verify(mockPreparedStatement).clearParameters();
        verify(mockPreparedStatement).close();
    }

    @Test
    void shouldHandleNullValuesAndSetSqlNull() throws SQLException {
        List<FireboltRecord> records = new ArrayList<>();
        records.add(buildRecord(TABLE_NAME, 0, 100L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(Integer.valueOf(1)).schemaType(Schema.Type.INT32).build(),
                "name", KafkaMessageColumnValue.builder().value("Carol").schemaType(Schema.Type.STRING).build()
        )));
        // name missing -> should be setNull
        records.add(buildRecord(TABLE_NAME, 0, 101L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(Integer.valueOf(2)).schemaType(Schema.Type.INT32).build()
        )));

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        verify(mockConnection).prepareStatement(argThat(sqlContains("INSERT INTO \"test_table\" (\"id\", \"NAME\") VALUES (?, ?)")));
        verify(mockPreparedStatement).setInt(1, 1);
        verify(mockPreparedStatement).setString(2, "Carol");

        verify(mockPreparedStatement).setInt(1, 2);
        verify(mockPreparedStatement).setNull(2, Types.VARCHAR);
    
        verify(mockPreparedStatement, times(2)).addBatch();
        verify(mockPreparedStatement).executeBatch();
        verify(mockPreparedStatement).clearBatch();
        verify(mockPreparedStatement).clearParameters();
        verify(mockPreparedStatement).close();
    }

    @Test
    void shouldIgnoreExtraColumnsNotInSchema() throws SQLException {
        Map<String, KafkaMessageColumnValue> values = new HashMap<>();
        values.put("id", KafkaMessageColumnValue.builder().value(100).schemaType(Schema.Type.INT32).build());
        values.put("name", KafkaMessageColumnValue.builder().value("widget").schemaType(Schema.Type.STRING).build());
        values.put("unknown", KafkaMessageColumnValue.builder().value("ignored").schemaType(Schema.Type.STRING).build());

        List<FireboltRecord> records = List.of(buildRecord(TABLE_NAME, 0, 1L, values));

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        verify(mockConnection).prepareStatement(argThat(sqlContains("INSERT INTO \"test_table\" (\"id\", \"NAME\") VALUES (?, ?)")));
        verify(mockPreparedStatement).setInt(1, 100);
        verify(mockPreparedStatement).setString(2, "widget");
        verify(mockPreparedStatement).addBatch();
        verify(mockPreparedStatement).executeBatch();
    }

    @Test
    void willOnlyCreatePreparedStatementWithColumnsInRecordsIfNotAllColumnsArePresent() throws SQLException {
        Map<String, KafkaMessageColumnValue> values = new HashMap<>();
        values.put("id", KafkaMessageColumnValue.builder().value(100).schemaType(Schema.Type.INT32).build());

        List<FireboltRecord> records = List.of(buildRecord(TABLE_NAME, 0, 1L, values));

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        verify(mockConnection).prepareStatement(argThat(sqlContains("INSERT INTO \"test_table\" (\"id\") VALUES (?)")));
        verify(mockPreparedStatement).setInt(1, 100);
        verify(mockPreparedStatement).addBatch();
        verify(mockPreparedStatement).executeBatch();
    }

    private static FireboltRecord buildRecord(String tableName, int partition, long offset,
                                              Map<String, KafkaMessageColumnValue> values) {
        new SinkRecord("topic", 1, null, null, null, null, 1234L);
        return new FireboltRecord(
                tableName,
                values,
                new SinkRecord("topic", partition, null, null, null, null, offset)
        );
    }

    @SafeVarargs
    private static Map<String, KafkaMessageColumnValue> mapOf(Object... keyVals) {
        Map<String, KafkaMessageColumnValue> map = new HashMap<>();
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = (String) keyVals[i];
            KafkaMessageColumnValue val = (KafkaMessageColumnValue) keyVals[i + 1];
            map.put(key, val);
        }
        return map;
    }

    private static ArgumentMatcher<String> sqlContains(String expected) {
        return sql -> sql != null && sql.contains(expected);
    }
}


