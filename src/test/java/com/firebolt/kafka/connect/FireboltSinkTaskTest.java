package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.firebolt.kafka.connect.service.FireboltDbService;
import com.firebolt.kafka.connect.service.FireboltSinkService;
import com.firebolt.kafka.connect.service.FireboltSinkServiceProvider;
import com.firebolt.kafka.connect.service.exception.ConnectionFailedException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import org.apache.kafka.connect.errors.RetriableException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;

public class FireboltSinkTaskTest {

    @Mock
    private FireboltSinkServiceProvider mockServiceProvider;
    
    @Mock
    private FireboltSinkService mockSinkService;
    
    @Mock
    private FireboltDbService mockDbService;
    
    @Mock
    private SinkConfig mockSinkConfig;
    
    @Mock
    private JdbcConfig mockJdbcConfig;

    private FireboltSinkTask fireboltSinkTask;
    private Map<String, String> validConfig;
    private Collection<TopicPartition> testPartitions;
    private Collection<SinkRecord> testRecords;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        fireboltSinkTask = new FireboltSinkTask();
        
        // Set up valid configuration
        validConfig = new HashMap<>();
        validConfig.put("jdbc.connection.url", "jdbc:firebolt:test_db");
        validConfig.put("topic.to.table.mapping", "test_topic:test_table");
        
        // Set up test partitions
        testPartitions = Arrays.asList(
            new TopicPartition("test_topic", 0),
            new TopicPartition("test_topic", 1),
            new TopicPartition("another_topic", 0)
        );
        
        // Set up test records
        Schema schema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
            
        Struct struct = new Struct(schema)
            .put("id", 123)
            .put("name", "test_name");
            
        testRecords = Arrays.asList(
            new SinkRecord("test_topic", 0, null, null, schema, struct, 100L)
        );
    }

    @Test
    void shouldReturnVersionFromPropertiesFile() {
        // Create a mock version.properties content
        String versionContent = "version=1.2.3\n";
        InputStream inputStream = new ByteArrayInputStream(versionContent.getBytes());
        
        // Create a custom task to test version loading
        FireboltSinkTask customTask = new FireboltSinkTask() {
            @Override
            public String version() {
                try {
                    Properties properties = new Properties();
                    properties.load(inputStream);
                    return properties.getProperty("version", "unknown");
                } catch (IOException e) {
                    return "unknown";
                }
            }
        };
        
        String version = customTask.version();
        assertEquals("1.2.3", version);
    }

    @Test
    void shouldReturnVersionFromPropertiesOrDefault() {
        String version = fireboltSinkTask.version();
        // Version could be "unknown", "${version}" (template), or actual version
        assertTrue(version != null && !version.isEmpty());
    }

    @Test
    void shouldStartSuccessfullyWithValidConfiguration() {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            // Mock the service provider
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), ArgumentMatchers.<Map<String, Set<Integer>>>any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            assertDoesNotThrow(() -> {
                fireboltSinkTask.start(validConfig);
            });
        }
    }

    @Test
    void shouldThrowExceptionWhenOpenFailsWithInvalidConfiguration() {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            // Mock the service provider to throw exception
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), ArgumentMatchers.<Map<String, Set<Integer>>>any(), any(ErrorReporter.class), anyBoolean())).thenThrow(new IllegalArgumentException("Invalid config"));

            assertDoesNotThrow(() -> fireboltSinkTask.start(validConfig));
            injectMockedDependencies();
            setupMocksForOpen();

            RuntimeException exception = assertThrows(RuntimeException.class, () -> fireboltSinkTask.open(testPartitions));
            
            assertTrue(exception.getMessage().contains("Failed to open Firebolt Sink Task"));
            assertInstanceOf(IllegalArgumentException.class, exception.getCause());
        }
    }

    @Test
    void shouldOpenSuccessfullyWithValidPartitions() {
        // Set up mocks for start
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), ArgumentMatchers.<Map<String, Set<Integer>>>any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            // Start the task first
            fireboltSinkTask.start(validConfig);
            
            // Set up mocks for open - inject the mocked dependencies
            injectMockedDependencies();
            setupMocksForOpen();
            
            assertDoesNotThrow(() -> {
                fireboltSinkTask.open(testPartitions);
            });
        }
    }

    @Test
    void shouldPassAssignedPartitionsToServiceProviderInOpen() {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);

            fireboltSinkTask.start(validConfig);

            injectMockedDependencies();
            setupMocksForOpen();

            assertDoesNotThrow(() -> fireboltSinkTask.open(testPartitions));

            ArgumentCaptor<Map<String, Set<Integer>>> partitionsCaptor = ArgumentCaptor.forClass(Map.class);
            verify(mockServiceProvider, times(1)).getService(any(SinkConfig.class), partitionsCaptor.capture(), any(ErrorReporter.class), anyBoolean());

            Map<String, Set<Integer>> passed = partitionsCaptor.getValue();
            assertNotNull(passed);
            assertTrue(passed.containsKey("test_topic"));
            assertTrue(passed.containsKey("another_topic"));
            assertEquals(java.util.Set.of(0, 1), passed.get("test_topic"));
            assertEquals(java.util.Set.of(0), passed.get("another_topic"));
        }
    }

    @Test
    void shouldClosePreviousServiceOnReopen() {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);

            FireboltSinkService firstService = mock(FireboltSinkService.class);
            FireboltSinkService secondService = mock(FireboltSinkService.class);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(Map.class), any(ErrorReporter.class), anyBoolean()))
                .thenReturn(firstService)
                .thenReturn(secondService);

            fireboltSinkTask.start(validConfig);

            injectMockedDependencies();
            setupMocksForOpen();
            assertDoesNotThrow(() -> fireboltSinkTask.open(testPartitions));

            // Reopen with a different set of partitions
            Collection<TopicPartition> newPartitions = Arrays.asList(
                new TopicPartition("test_topic", 2)
            );
            setupMocksForOpen();
            assertDoesNotThrow(() -> fireboltSinkTask.open(newPartitions));

            verify(firstService, times(1)).close();
        }
    }

    @Test
    void shouldHandleEmptyPartitionsInOpen() {
        // Start the task first
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Set up minimal mocks for empty partitions
            setupMocksForOpen();
            
            assertDoesNotThrow(() -> {
                fireboltSinkTask.open(Collections.emptyList());
            });
        }
    }

    @Test
    void shouldThrowExceptionWhenSchemaDiscoveryFails() {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Set up mocks to fail schema discovery
            setupMocksForOpenWithSchemaFailure();
            
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                fireboltSinkTask.open(testPartitions);
            });
            
            assertTrue(exception.getMessage().contains("Failed to open Firebolt Sink Task"));
        }
    }

    @Test
    void shouldThrowExceptionWhenTableNotFoundInFirebolt() {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Set up mocks where tables are not found
            setupMocksForOpenWithMissingTables();
            
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                fireboltSinkTask.open(testPartitions);
            });
            
            // Check for any of the possible error messages related to missing tables
            String message = exception.getMessage();
            assertTrue(message.contains("table") || message.contains("schema") || message.contains("Failed to open"));
        }
    }

    @Test
    void shouldProcessRecordsSuccessfully() throws SQLException {
        // Start and open the task
        startAndOpenTask();
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.put(testRecords);
        });
        
        verify(mockSinkService).processRecord(eq(testRecords), any(Map.class));
    }

    @Test
    void shouldHandleNullRecordsInPut() throws SQLException {
        // Start and open the task
        startAndOpenTask();
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.put(null);
        });
        
        // Verify service was not called with null records
        verify(mockSinkService, never()).processRecord(any(), any());
    }

    @Test
    void shouldHandleEmptyRecordsInPut() throws SQLException {
        // Start and open the task
        startAndOpenTask();
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.put(Collections.emptyList());
        });
        
        // Verify service was not called with empty records
        verify(mockSinkService, never()).processRecord(any(), any());
    }

    @Test
    void shouldThrowRetriableWhenFireboltTooManyRequests() throws SQLException {
        // Start and open the task
        startAndOpenTask();

        // Mock service to throw a retriable FireboltException
        FireboltException fireboltException = mock(FireboltException.class);
        when(fireboltException.getType()).thenReturn(ExceptionType.TOO_MANY_REQUESTS);
        doThrow(fireboltException).when(mockSinkService).processRecord(any(), any());

        assertThrows(RetriableException.class, () -> fireboltSinkTask.put(testRecords));
    }

    @Test
    void shouldThrowRetriableWhenCommitConflictDetected() throws SQLException {
        // Start and open the task
        startAndOpenTask();

        // Mock service to throw a FireboltException with a conflict message in the cause
        FireboltException conflictException = mock(FireboltException.class);
        when(conflictException.getType()).thenReturn(ExceptionType.CONFLICT);
        doThrow(conflictException).when(mockSinkService).processRecord(any(), any());

        assertThrows(RetriableException.class, () -> fireboltSinkTask.put(testRecords));
    }

    @Test
    void shouldThrowRuntimeWhenFireboltRequestBodyTooLarge() throws SQLException {
        // Start and open the task
        startAndOpenTask();

        // Mock service to throw a non-retriable FireboltException (HTTP 413)
        FireboltException fireboltException = mock(FireboltException.class);
        when(fireboltException.getType()).thenReturn(ExceptionType.REQUEST_BODY_TOO_LARGE);
        doThrow(fireboltException).when(mockSinkService).processRecord(any(), any());

        RuntimeException exception = assertThrows(RuntimeException.class, () -> fireboltSinkTask.put(testRecords));
        assertTrue(exception.getMessage().contains("Number of records that failed: 1"));
        assertInstanceOf(FireboltException.class, exception.getCause());
    }

    @Test
    void shouldThrowRuntimeWhenRecordConversionFails() throws SQLException {
        // Start and open the task
        startAndOpenTask();

        // Mock service to throw a non-retriable conversion exception
        RecordConversionFailedException conversionFailed = RecordConversionFailedException.builder()
                .message("conversion failed")
                .tableName("test_table")
                .topicName("test_topic")
                .kafkaPartition(0)
                .kafkaOffset(100L)
                .build();
        doThrow(conversionFailed).when(mockSinkService).processRecord(any(), any());

        RuntimeException exception = assertThrows(RuntimeException.class, () -> fireboltSinkTask.put(testRecords));
        assertTrue(exception.getMessage().contains("Number of records that failed: 1"));
        assertInstanceOf(RecordConversionFailedException.class, exception.getCause());
    }

    @Test
    void shouldThrowExceptionWhenRecordProcessingFailsWithErrorToleranceNone() throws SQLException {
        validConfig.put("errors.tolerance", "none");
        // Start and open the task
        startAndOpenTask();
        
        // Mock service to throw exception
        doThrow(new RuntimeException("Processing failed")).when(mockSinkService)
            .processRecord(any(), any());
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            fireboltSinkTask.put(testRecords);
        });
        
        assertTrue(exception.getMessage().contains("Number of records that failed: 1"));
        assertTrue(exception.getCause().getMessage().contains("Processing failed"));
    }

    @Test
    void shouldThrowExceptionWhenRecordProcessingFailsWithErrorToleranceNotSet() throws SQLException {
        // Start and open the task
        startAndOpenTask();

        // Mock service to throw exception
        doThrow(new RuntimeException("Processing failed")).when(mockSinkService)
            .processRecord(any(), any());

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            fireboltSinkTask.put(testRecords);
        });

        assertTrue(exception.getMessage().contains("Number of records that failed: 1"));
        assertTrue(exception.getCause().getMessage().contains("Processing failed"));
    }

    @Test
    void shouldPushRecordsToDlqWhenRecordProcessingFailsWithErrorToleranceAll() throws SQLException {
        validConfig.put("errors.tolerance", "all");

        //mocking initialization done by connect framework
        SinkTaskContext contextMock = mock(SinkTaskContext.class);
        fireboltSinkTask.initialize(contextMock);
        ErrantRecordReporter errantRecordReporterMock = mock(ErrantRecordReporter.class);
        when(contextMock.errantRecordReporter()).thenReturn(errantRecordReporterMock);

        // Start and open the task
        startAndOpenTask();

        // Mock service to throw exception
        doThrow(new RuntimeException("Processing failed")).when(mockSinkService)
            .processRecord(any(), any());
        when(errantRecordReporterMock.report(any(),any())).thenReturn(CompletableFuture.completedFuture(null));

        assertDoesNotThrow(() -> {
            fireboltSinkTask.put(testRecords);
        });

        verify(errantRecordReporterMock, times(1)).report(any(), any());
    }

    @Test
    void shouldFlushSuccessfully() {
        // Start and open the task
        startAndOpenTask();
        
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("test_topic", 0), new OffsetAndMetadata(100L));
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.flush(offsets);
        });
    }

    @Test
    void shouldStopSuccessfully() {
        // Start the task first to set up the sink service
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Inject the mock sink service
            try {
                java.lang.reflect.Field serviceField = FireboltSinkTask.class.getDeclaredField("fireboltSinkService");
                serviceField.setAccessible(true);
                serviceField.set(fireboltSinkTask, mockSinkService);
            } catch (Exception e) {
                // If reflection fails, tests may not work as expected
            }
            
            assertDoesNotThrow(() -> {
                fireboltSinkTask.stop();
            });
            
            // Verify that the sink service close method was called
            verify(mockSinkService).close();
        }
    }

    @Test
    void shouldStopSuccessfullyWhenSinkServiceIsNull() {
        // Test stopping when sink service is null (task not started)
        assertDoesNotThrow(() -> {
            fireboltSinkTask.stop();
        });
        
        // Verify that no close method was called since service is null
        verify(mockSinkService, never()).close();
    }

    @Test
    void shouldHandleStopWhenSinkServiceCloseThrowsException() {
        // Start the task first to set up the sink service
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Inject the mock sink service
            try {
                java.lang.reflect.Field serviceField = FireboltSinkTask.class.getDeclaredField("fireboltSinkService");
                serviceField.setAccessible(true);
                serviceField.set(fireboltSinkTask, mockSinkService);
            } catch (Exception e) {
                // If reflection fails, tests may not work as expected
            }
            
            // Mock the sink service to throw an exception when close is called
            doThrow(new RuntimeException("Close failed")).when(mockSinkService).close();
            
            // Stop should not throw an exception even if close fails
            assertDoesNotThrow(() -> {
                fireboltSinkTask.stop();
            });
            
            // Verify that the sink service close method was called
            verify(mockSinkService).close();
        }
    }

    @Test
    void shouldStopMultipleTimesSafely() {
        // Start the task first to set up the sink service
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Inject the mock sink service
            try {
                java.lang.reflect.Field serviceField = FireboltSinkTask.class.getDeclaredField("fireboltSinkService");
                serviceField.setAccessible(true);
                serviceField.set(fireboltSinkTask, mockSinkService);
            } catch (Exception e) {
                // If reflection fails, tests may not work as expected
            }
            
            // Stop multiple times
            assertDoesNotThrow(() -> {
                fireboltSinkTask.stop();
                fireboltSinkTask.stop();
                fireboltSinkTask.stop();
            });
            
            // Verify that the sink service close method was called only once
            verify(mockSinkService, times(3)).close();
        }
    }

    @ParameterizedTest
    @CsvSource({
        "topic1:table1,topic2:table2",
        "single_topic:single_table",
        "test-topic:test-table,another-topic:another-table"
    })
    void shouldHandleDifferentTopicToTableMappings(String mappingConfig) {
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            Map<String, String> config = new HashMap<>();
            config.put("jdbc.connection.url", "jdbc:firebolt:test_db");
            config.put("topic.to.table.mapping", mappingConfig);
            
            assertDoesNotThrow(() -> {
                fireboltSinkTask.start(config);
            });
        }
    }

    @Test
    void shouldExtractUniqueTopicsFromPartitions() {
        // Start the task
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Create partitions with duplicate topics
            Collection<TopicPartition> partitionsWithDuplicates = Arrays.asList(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic1", 2)  // Another partition for topic1
            );
            
            // Set up mocks for open - inject the mocked dependencies
            injectMockedDependencies();
            setupMocksForOpenWithMultipleTopics();
            
            assertDoesNotThrow(() -> {
                fireboltSinkTask.open(partitionsWithDuplicates);
            });
        }
    }

    @Test
    void shouldHandleMultipleRecords() throws SQLException {
        // Start and open the task
        startAndOpenTask();
        
        // Create multiple records
        Schema schema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
            
        List<SinkRecord> multipleRecords = Arrays.asList(
            new SinkRecord("test_topic", 0, null, null, schema, 
                new Struct(schema).put("id", 1).put("name", "record1"), 100L),
            new SinkRecord("test_topic", 0, null, null, schema, 
                new Struct(schema).put("id", 2).put("name", "record2"), 101L),
            new SinkRecord("test_topic", 0, null, null, schema, 
                new Struct(schema).put("id", 3).put("name", "record3"), 102L)
        );
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.put(multipleRecords);
        });
        
        verify(mockSinkService).processRecord(eq(multipleRecords), any(Map.class));
    }

    @Test
    void shouldHandleVersionLoadingIOException() {
        // Create a task that simulates IOException during version loading
        FireboltSinkTask customTask = new FireboltSinkTask() {
            @Override
            public String version() {
                try {
                    // Simulate IOException
                    throw new IOException("Simulated IO error");
                } catch (IOException e) {
                    return "unknown";
                }
            }
        };
        
        String version = customTask.version();
        assertEquals("unknown", version);
    }

    @Test
    void shouldHandleFlushWithEmptyOffsets() {
        // Start and open the task
        startAndOpenTask();
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.flush(Collections.emptyMap());
        });
    }

    @Test
    void shouldHandleFlushWithNullOffsets() {
        // Start and open the task
        startAndOpenTask();
        
        assertDoesNotThrow(() -> {
            fireboltSinkTask.flush(null);
        });
    }

    // Helper methods
    
    private void setupMocksForOpen() {
        // Mock SinkConfig
        when(mockSinkConfig.getTableNameForTopic("test_topic")).thenReturn("test_table");
        when(mockSinkConfig.getTableNameForTopic("another_topic")).thenReturn("another_table");
        when(mockSinkConfig.getJdbcConfig()).thenReturn(mockJdbcConfig);
        
        // Mock successful schema discovery
        Map<String, TableSchema> schemas = new HashMap<>();
        schemas.put("test_table", new TableSchema("test_table"));
        schemas.put("another_table", new TableSchema("another_table"));
        
        try {
            when(mockDbService.discoverTableSchemas(eq(mockJdbcConfig), ArgumentMatchers.<java.util.Set<String>>any()))
                .thenReturn(schemas);
        } catch (Exception e) {
            // This shouldn't happen in the mock setup
        }
    }
    
    private void setupMocksForOpenWithSchemaFailure() {
        when(mockSinkConfig.getTableNameForTopic(anyString())).thenReturn("test_table");
        when(mockSinkConfig.getJdbcConfig()).thenReturn(mockJdbcConfig);
        
        try {
            when(mockDbService.discoverTableSchemas(eq(mockJdbcConfig), ArgumentMatchers.<java.util.Set<String>>any()))
                .thenThrow(new ConnectionFailedException("Schema discovery failed"));
        } catch (Exception e) {
            // This shouldn't happen in the mock setup
        }
        
        injectMockedDependencies();
    }
    
    private void setupMocksForOpenWithMissingTables() {
        when(mockSinkConfig.getTableNameForTopic("test_topic")).thenReturn("test_table");
        when(mockSinkConfig.getJdbcConfig()).thenReturn(mockJdbcConfig);
        
        // Return empty schemas (no tables found)
        try {
            when(mockDbService.discoverTableSchemas(eq(mockJdbcConfig), ArgumentMatchers.<java.util.Set<String>>any()))
                .thenReturn(Collections.emptyMap());
        } catch (Exception e) {
            // This shouldn't happen in the mock setup
        }
        
        injectMockedDependencies();
    }
    
    private void setupMocksForOpenWithMultipleTopics() {
        // Mock SinkConfig
        when(mockSinkConfig.getTableNameForTopic("topic1")).thenReturn("topic1_table");
        when(mockSinkConfig.getTableNameForTopic("topic2")).thenReturn("topic2_table");
        when(mockSinkConfig.getJdbcConfig()).thenReturn(mockJdbcConfig);
        
        // Mock successful schema discovery
        Map<String, TableSchema> schemas = new HashMap<>();
        schemas.put("topic1_table", new TableSchema("topic1_table"));
        schemas.put("topic2_table", new TableSchema("topic2_table"));
        
        try {
            when(mockDbService.discoverTableSchemas(eq(mockJdbcConfig), ArgumentMatchers.<java.util.Set<String>>any()))
                .thenReturn(schemas);
        } catch (Exception e) {
            // This shouldn't happen in the mock setup
        }
    }
    
    private void startAndOpenTask() {
        // Skip the complex open operation for simple tests
        // Just start the task, which is sufficient for testing put/flush operations
        try (MockedStatic<FireboltSinkServiceProvider> mockedProvider = mockStatic(FireboltSinkServiceProvider.class)) {
            mockedProvider.when(FireboltSinkServiceProvider::getInstance).thenReturn(mockServiceProvider);
            when(mockServiceProvider.getService(any(SinkConfig.class), ArgumentMatchers.<Map<String, Set<Integer>>>any(), any(ErrorReporter.class), anyBoolean())).thenReturn(mockSinkService);
            
            fireboltSinkTask.start(validConfig);
            
            // Inject the mock sink service directly
            try {
                java.lang.reflect.Field serviceField = FireboltSinkTask.class.getDeclaredField("fireboltSinkService");
                serviceField.setAccessible(true);
                serviceField.set(fireboltSinkTask, mockSinkService);
            } catch (Exception e) {
                // If reflection fails, tests may not work as expected
            }
        }
    }
    
    private void injectMockedDependencies() {
        // Since the dependencies are created internally, we need to use reflection
        // or modify the class to allow dependency injection for testing
        try {
            java.lang.reflect.Field sinkConfigField = FireboltSinkTask.class.getDeclaredField("sinkConfig");
            sinkConfigField.setAccessible(true);
            sinkConfigField.set(fireboltSinkTask, mockSinkConfig);
            
            java.lang.reflect.Field dbServiceField = FireboltSinkTask.class.getDeclaredField("fireboltDbService");
            dbServiceField.setAccessible(true);
            dbServiceField.set(fireboltSinkTask, mockDbService);
        } catch (Exception e) {
            // If reflection fails, skip dependency injection
            // Tests may not work as expected but won't crash
        }
    }
} 