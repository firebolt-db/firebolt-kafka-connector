package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class FireboltWriterTest {

    @Mock
    private SinkConfig mockSinkConfig;
    
    @Mock
    private Connection mockConnection;
    
    @Mock
    private PreparedStatement mockPreparedStatement;
    
    @Mock
    private ColumnDataTypeConverterFactory mockConverterFactory;
    
    @Mock
    private ColumnDataTypeConverter mockConverter;

    private FireboltWriter fireboltWriter;
    private TableSchema testTableSchema;
    private FireboltRecord testRecord;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        
        // Set up test table schema
        testTableSchema = new TableSchema("test_table");
        testTableSchema.addColumn("id", "INTEGER", 4, false);
        testTableSchema.addColumn("name", "TEXT", 12, true);
        
        // Set up test record
        Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
        columnValues.put("id", KafkaMessageColumnValue.builder().value(123L).build());
        columnValues.put("name", KafkaMessageColumnValue.builder().value("test_name").build());
        
        testRecord = new FireboltRecord(
            "test_table",
            columnValues,
            "test_topic",
            0,
            100L,
            System.currentTimeMillis()
        );
        
        // Mock PreparedStatement creation
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{1});
        
        fireboltWriter = new FireboltWriter(mockSinkConfig, mockConnection);
    }
    
    private void executeWithMockedConverter(Runnable testCode) {
        try (MockedStatic<ColumnDataTypeConverterFactory> mockedFactory = mockStatic(ColumnDataTypeConverterFactory.class)) {
            // Mock the converter factory
            mockedFactory.when(ColumnDataTypeConverterFactory::getInstance).thenReturn(mockConverterFactory);
            when(mockConverterFactory.getConverter(any(TableSchema.Column.class))).thenReturn(mockConverter);
            
            testCode.run();
        }
    }

    @Test
    void shouldCreateFireboltWriterWithValidConfiguration() {
        assertNotNull(fireboltWriter);
    }

    @Test
    void shouldWriteRecordToBatch() {
        assertDoesNotThrow(() -> {
            fireboltWriter.write(testRecord, testTableSchema);
        });
    }

    @Test
    void shouldFlushEmptyBatchWithoutError() {
        assertDoesNotThrow(() -> fireboltWriter.flush());
        
        // Verify no SQL operations were performed on empty batch
        try {
            verify(mockConnection, never()).prepareStatement(anyString());
        } catch (Exception e) {
            // Ignore verification exceptions for empty batch test
        }
    }

    @Test
    void shouldFlushRecordsAndClearBatch() {
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(testRecord, testTableSchema);
                fireboltWriter.flush();
                
                // Verify PreparedStatement was created and executed
                verify(mockConnection, atLeastOnce()).prepareStatement(anyString());
                verify(mockPreparedStatement).executeBatch();
                verify(mockPreparedStatement).clearBatch();
                verify(mockPreparedStatement).clearParameters();
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldCreateCorrectInsertSQL() {
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(testRecord, testTableSchema);
                fireboltWriter.flush();
                
                // Verify that column names are quoted in SQL and table name is included
                verify(mockConnection).prepareStatement(argThat(sql -> 
                    sql.contains("INSERT INTO test_table") &&
                    sql.contains("\"id\"") && 
                    sql.contains("\"name\"")
                ));
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldClearTableSchemaCacheAndPreparedStatements() {
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(testRecord, testTableSchema);
                fireboltWriter.flush();
                
                // Clear cache
                fireboltWriter.clearTableSchemaCache();
                
                // Write again - should create new prepared statement
                fireboltWriter.write(testRecord, testTableSchema);
                fireboltWriter.flush();
                
                verify(mockConnection, atLeast(2)).prepareStatement(anyString());
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldCloseAndFlushRemainingRecords() {
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(testRecord, testTableSchema);
                
                fireboltWriter.close();
                
                // Verify final flush occurred
                verify(mockConnection, atLeastOnce()).prepareStatement(anyString());
                verify(mockPreparedStatement).executeBatch();
                
                // Verify prepared statement was closed
                verify(mockPreparedStatement).close();
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldHandleMultipleRecordsForSameTable() {
        executeWithMockedConverter(() -> {
            try {
                // Create multiple records for same table
                for (int i = 0; i < 3; i++) {
                    Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
                    columnValues.put("id", KafkaMessageColumnValue.builder().value((long) i).build());
                    columnValues.put("name", KafkaMessageColumnValue.builder().value("name_" + i).build());
                    
                    FireboltRecord record = new FireboltRecord(
                        "test_table",
                        columnValues,
                        "test_topic",
                        0,
                        (long) i,
                        System.currentTimeMillis()
                    );
                    
                    fireboltWriter.write(record, testTableSchema);
                }
                
                fireboltWriter.flush();
                
                // Verify batch operations
                verify(mockPreparedStatement, times(3)).addBatch();
                verify(mockPreparedStatement).executeBatch();
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldHandleSQLExceptionDuringFlush() throws SQLException {
        when(mockPreparedStatement.executeBatch()).thenThrow(new SQLException("Database error"));
        
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(testRecord, testTableSchema);
                
                RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                    fireboltWriter.flush();
                });
                
                assertTrue(exception.getMessage().contains("Database error") || 
                          exception.getCause() instanceof SQLException);
            } catch (Exception e) {
                // Expected for this test
            }
        });
    }

    @Test
    void shouldHandleCloseExceptionGracefully() {
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(testRecord, testTableSchema);
                fireboltWriter.flush();
                
                // Mock statement close to throw exception
                doThrow(new SQLException("Close failed")).when(mockPreparedStatement).close();
                
                // Close should not throw exception despite statement close failure
                assertDoesNotThrow(() -> fireboltWriter.close());
            } catch (Exception e) {
                fail("Unexpected exception in setup: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldHandleEmptyColumnValues() {
        Map<String, KafkaMessageColumnValue> emptyColumnValues = new HashMap<>();
        
        FireboltRecord emptyRecord = new FireboltRecord(
            "test_table",
            emptyColumnValues,
            "test_topic",
            0,
            100L,
            System.currentTimeMillis()
        );
        
        assertDoesNotThrow(() -> {
            fireboltWriter.write(emptyRecord, testTableSchema);
            fireboltWriter.flush();
        });
    }

    @ParameterizedTest
    @CsvSource({
        "test_table_1",
        "test_table_2", 
        "another_table"
    })
    void shouldHandleDifferentTableNames(String tableName) {
        TableSchema schema = new TableSchema(tableName);
        schema.addColumn("col1", "INTEGER", 4, false);
        
        Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
        columnValues.put("col1", KafkaMessageColumnValue.builder().value(123L).build());
        
        FireboltRecord record = new FireboltRecord(
            tableName,
            columnValues,
            "test_topic",
            0,
            100L,
            System.currentTimeMillis()
        );
        
        executeWithMockedConverter(() -> {
            try {
                fireboltWriter.write(record, schema);
                fireboltWriter.flush();
                
                verify(mockConnection).prepareStatement(contains(tableName));
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldFilterValidColumnsFromRecords() {
        executeWithMockedConverter(() -> {
            try {
                // Create record with extra column that doesn't exist in schema
                Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
                columnValues.put("id", KafkaMessageColumnValue.builder().value(123L).build());
                columnValues.put("name", KafkaMessageColumnValue.builder().value("test_name").build());
                columnValues.put("extra_column", KafkaMessageColumnValue.builder().value("extra_value").build());
                
                FireboltRecord recordWithExtraColumn = new FireboltRecord(
                    "test_table",
                    columnValues,
                    "test_topic",
                    0,
                    100L,
                    System.currentTimeMillis()
                );
                
                fireboltWriter.write(recordWithExtraColumn, testTableSchema);
                fireboltWriter.flush();
                
                // Verify SQL doesn't include the extra column
                verify(mockConnection).prepareStatement(argThat(sql -> 
                    !sql.contains("extra_column")
                ));
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldHandleCaseInsensitiveColumnNameMatching() {
        executeWithMockedConverter(() -> {
            try {
                // Create table schema with mixed case column names
                TableSchema caseTestSchema = new TableSchema("case_test_table");
                caseTestSchema.addColumn("ID", "INTEGER", 4, false); // uppercase
                caseTestSchema.addColumn("userName", "TEXT", 12, true); // camelCase
                caseTestSchema.addColumn("FULL_NAME", "TEXT", 12, true); // uppercase with underscore
                caseTestSchema.addColumn("CreatedDate", "TEXT", 12, true); // PascalCase
                
                // Create record with different case column names that should match
                Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
                columnValues.put("id", KafkaMessageColumnValue.builder().value(123L).build()); // lowercase vs "ID"
                columnValues.put("USERNAME", KafkaMessageColumnValue.builder().value("john_doe").build()); // uppercase vs "userName"
                columnValues.put("full_name", KafkaMessageColumnValue.builder().value("John Doe").build()); // lowercase vs "FULL_NAME"
                columnValues.put("createddate", KafkaMessageColumnValue.builder().value("2024-01-01").build()); // lowercase vs "CreatedDate"
                
                FireboltRecord caseTestRecord = new FireboltRecord(
                    "case_test_table",
                    columnValues,
                    "test_topic",
                    0,
                    100L,
                    System.currentTimeMillis()
                );
                
                fireboltWriter.write(caseTestRecord, caseTestSchema);
                fireboltWriter.flush();
                
                // Verify SQL includes all columns with correct schema case (quoted)
                verify(mockConnection).prepareStatement(argThat(sql -> 
                    sql.contains("\"ID\"") &&           // Schema case preserved
                    sql.contains("\"userName\"") &&     // Schema case preserved
                    sql.contains("\"FULL_NAME\"") &&    // Schema case preserved
                    sql.contains("\"CreatedDate\"") &&    // Schema case preserved
                    !sql.contains("\"id\"") &&           // Not record case
                    !sql.contains("\"USERNAME\"") &&    // Not record case
                    !sql.contains("\"full_name\"") &&    // Not record case
                    !sql.contains("\"createddate\"")     // Not record case
                ));
                
                // Verify all 4 parameters are set via converter (case-insensitive matching worked)
                verify(mockConverter, times(4)).convertAndSet(eq(mockPreparedStatement), anyInt(), any(), any());
                
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @Test
    void shouldHandleCaseMismatchWithSomeUnmatchedColumns() {
        executeWithMockedConverter(() -> {
            try {
                // Create table schema
                TableSchema schema = new TableSchema("test_table");
                schema.addColumn("UserId", "INTEGER", 4, false);
                schema.addColumn("EMAIL", "TEXT", 12, true);
                
                // Create record with mixed case and extra columns
                Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
                columnValues.put("userid", KafkaMessageColumnValue.builder().value(456L).build()); // should match "UserId"
                columnValues.put("email", KafkaMessageColumnValue.builder().value("test@example.com").build()); // should match "EMAIL"
                columnValues.put("phone", KafkaMessageColumnValue.builder().value("123-456-7890").build()); // should be filtered out
                columnValues.put("address", KafkaMessageColumnValue.builder().value("123 Main St").build()); // should be filtered out
                
                FireboltRecord record = new FireboltRecord(
                    "test_table",
                    columnValues,
                    "test_topic",
                    0,
                    100L,
                    System.currentTimeMillis()
                );
                
                fireboltWriter.write(record, schema);
                fireboltWriter.flush();
                
                // Verify SQL includes only the matched columns with schema case
                verify(mockConnection).prepareStatement(argThat(sql -> 
                    sql.contains("\"UserId\"") &&
                    sql.contains("\"EMAIL\"") &&
                    !sql.contains("phone") &&
                    !sql.contains("address")
                ));
                
                // Verify only 2 parameters are set via converter (2 matched columns)
                verify(mockConverter, times(2)).convertAndSet(eq(mockPreparedStatement), anyInt(), any(), any());
                
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }

    @ParameterizedTest
    @CsvSource({
        "ID, id, true",           // uppercase to lowercase
        "userName, USERNAME, true", // camelCase to uppercase  
        "full_name, FULL_NAME, true", // lowercase to uppercase
        "CreatedAt, createdat, true", // PascalCase to lowercase
        "Email, email, true",     // PascalCase to lowercase
        "userId, UserId, true",   // camelCase to PascalCase
        "nonexistent, missing, false" // no match
    })
    void shouldMatchColumnNamesCaseInsensitively(String schemaColumn, String recordColumn, boolean shouldMatch) {
        executeWithMockedConverter(() -> {
            try {
                // Create table schema with the schema column
                TableSchema schema = new TableSchema("test_table");
                schema.addColumn(schemaColumn, "TEXT", 12, true);
                
                // Create record with the record column
                Map<String, KafkaMessageColumnValue> columnValues = new HashMap<>();
                columnValues.put(recordColumn, KafkaMessageColumnValue.builder().value("test_value").build());
                
                FireboltRecord record = new FireboltRecord(
                    "test_table",
                    columnValues,
                    "test_topic",
                    0,
                    100L,
                    System.currentTimeMillis()
                );
                
                fireboltWriter.write(record, schema);
                fireboltWriter.flush();
                
                if (shouldMatch) {
                    // Verify SQL includes the schema column name (quoted)
                    verify(mockConnection).prepareStatement(argThat(sql -> 
                        sql.contains("\"" + schemaColumn + "\"")
                    ));
                    
                    // Verify parameter is set via converter
                    verify(mockConverter, atLeastOnce()).convertAndSet(eq(mockPreparedStatement), anyInt(), any(), any());
                } else {
                    // For non-matching case, verify no columns in SQL
                    verify(mockConnection).prepareStatement(argThat(sql -> 
                        sql.matches("INSERT INTO test_table \\(\\) VALUES \\(\\)")
                    ));
                }
                
            } catch (SQLException e) {
                fail("Unexpected SQLException: " + e.getMessage());
            }
        });
    }
} 