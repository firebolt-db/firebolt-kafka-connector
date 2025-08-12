package com.firebolt.kafka.connect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.argThat;
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

        insertPreparedStatement = new InsertPreparedStatement(mockConnection, mockTableSchema);
    }

    @Test
    void shouldBuildCorrectInsertSQLAndSetParametersWithCaseInsensitiveColumnNames() throws SQLException {        
        List<FireboltRecord> records = new ArrayList<>();
        records.add(buildRecord(TABLE_NAME, 0, 10L, mapOf(
                "ID", KafkaMessageColumnValue.builder().value(123L).schemaType(Schema.Type.INT64).build(),
                "name", KafkaMessageColumnValue.builder().value("Alice").schemaType(Schema.Type.STRING).build()
        )));
        records.add(buildRecord(TABLE_NAME, 0, 11L, mapOf(
                "ID", KafkaMessageColumnValue.builder().value(456L).schemaType(Schema.Type.INT64).build(),
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
                "id", KafkaMessageColumnValue.builder().value(1L).schemaType(Schema.Type.INT64).build(),
                "name", KafkaMessageColumnValue.builder().value("Carol").schemaType(Schema.Type.STRING).build()
        )));
        // name missing -> should be setNull
        records.add(buildRecord(TABLE_NAME, 0, 101L, mapOf(
                "id", KafkaMessageColumnValue.builder().value(2L).schemaType(Schema.Type.INT64).build()
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
        values.put("id", KafkaMessageColumnValue.builder().value(100L).schemaType(Schema.Type.INT64).build());
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
        values.put("id", KafkaMessageColumnValue.builder().value(100L).schemaType(Schema.Type.INT64).build());

        List<FireboltRecord> records = List.of(buildRecord(TABLE_NAME, 0, 1L, values));

        assertDoesNotThrow(() -> insertPreparedStatement.addRecords(records));

        verify(mockConnection).prepareStatement(argThat(sqlContains("INSERT INTO \"test_table\" (\"id\") VALUES (?)")));
        verify(mockPreparedStatement).setInt(1, 100);
        verify(mockPreparedStatement).addBatch();
        verify(mockPreparedStatement).executeBatch();
    }

    private static FireboltRecord buildRecord(String tableName, int partition, long offset,
                                              Map<String, KafkaMessageColumnValue> values) {
        return new FireboltRecord(
                tableName,
                values,
                "topic",
                partition,
                offset,
                System.currentTimeMillis()
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


