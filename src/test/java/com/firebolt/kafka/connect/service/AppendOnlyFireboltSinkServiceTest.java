package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.TableWriter;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AppendOnlyFireboltSinkServiceTest {

    private static final String TOPIC_A = "topicA";
    private static final String TOPIC_B = "topicB";
    private static final String TABLE_A = "tableA";
    private static final String TABLE_B = "tableB";

    @Mock
    private TableSchema mockSchemaTableA;
    @Mock
    private TableSchema mockSchemaTableB;

    @Mock
    private SinkConfig mockSinkConfig;

    @Mock
    private FireboltDbService mockDbService;

    @Mock
    private ErrorReporter mockErrorReporter;

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private TableWriterProvider mockTableWriterProvider;

    @Captor
    private ArgumentCaptor<List<SinkRecord>> tableARecordListCaptor;

    @Captor
    private ArgumentCaptor<List<SinkRecord>> tableBRecordListCaptor;

    private AppendOnlyFireboltSinkService service;
    private Map<String, TableWriter> tableWriterMap;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        tableWriterMap = new HashMap<>();
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, tableWriterMap, Map.of(TOPIC_A, Set.of(0), TOPIC_B, Set.of(0)), mockErrorReporter, false, mockTableWriterProvider);

        when(mockSinkConfig.isExactlyOnce()).thenReturn(false);
        when(mockSinkConfig.getTableNameForTopic(TOPIC_A)).thenReturn(TABLE_A);
        when(mockSinkConfig.getTableNameForTopic(TOPIC_B)).thenReturn(TABLE_B);
        when(mockSinkConfig.getJdbcConfig()).thenReturn(null);

        when(mockDbService.createConnection(any())).thenReturn(mockConnection);

        when(mockSchemaTableA.getTableName()).thenReturn(TABLE_A);
        when(mockSchemaTableB.getTableName()).thenReturn(TABLE_B);
    }

    @Test
    void shouldReturnWhenNoRecords() {
        assertDoesNotThrow(() -> service.processRecord(List.of(), Map.of()));
        verify(mockSinkConfig, times(1)).isExactlyOnce();
        verifyNoMoreInteractions(mockSinkConfig);
        verifyNoInteractions(mockDbService, mockTableWriterProvider);
    }

    @Test
    void shouldIgnoreRecordsWhenTableSchemaMissing()  {
        SinkRecord rec = buildRecord(TOPIC_A, 0, 1L);
        Map<String, TableSchema> schemas = Map.of();

        // No schema for the table -> no writer should be created and nothing inserted
        assertDoesNotThrow(() -> service.processRecord(List.of(rec), schemas));
        verifyNoInteractions(mockTableWriterProvider);
    }

    @Test
    void shouldGroupByTopicAndInsertRecords() throws Exception {
        // two topics
        SinkRecord recA1 = buildRecord(TOPIC_A, 0, 1L);
        SinkRecord recA2 = buildRecord(TOPIC_A, 0, 2L);
        SinkRecord recB1 = buildRecord(TOPIC_B, 0, 5L);

        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA, TABLE_B, mockSchemaTableB);

        // Provide pre-created writers to avoid DB work and to verify inserts per table
        TableWriter writerA = mock(TableWriter.class);
        TableWriter writerB = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        when(writerB.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);
        tableWriterMap.put(TABLE_B, writerB);

        // process
        assertDoesNotThrow(() -> service.processRecord(List.of(recA1, recA2, recB1), schemas));
        verify(writerA).insertRecords(tableARecordListCaptor.capture());
        verify(writerB).insertRecords(tableBRecordListCaptor.capture());

        List<SinkRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(2, tableARecords.size());
        assertEquals(List.of(recA1, recA2), tableARecords);

        List<SinkRecord> tableBRecords = tableBRecordListCaptor.getValue();
        assertEquals(1, tableBRecords.size());
        assertEquals(List.of(recB1), tableBRecords);

        verifyNoInteractions(mockErrorReporter);
    }

    @Test
    void shouldUseDefaultOffsetsWhenNotExactlyOnce() throws Exception {
        when(mockSinkConfig.isExactlyOnce()).thenReturn(false);

        // Writer created lazily through the provider should receive -1 offsets for the assigned partitions
        TableWriter writer = mock(TableWriter.class);
        when(writer.getProcessedPartitionOffsets()).thenReturn(Map.of(0, -1L));
        when(mockTableWriterProvider.get(
                eq(mockSchemaTableA),
                any(),
                any(),
                eq(TOPIC_A),
                eq(Map.of(0, -1L)),
                eq(mockErrorReporter),
                any()
        )).thenReturn(writer);

        SinkRecord recA = buildRecord(TOPIC_A, 0, 1L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        assertDoesNotThrow(() -> service.processRecord(List.of(recA), schemas));

        verify(writer).insertRecords(tableARecordListCaptor.capture());
        List<SinkRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(1, tableARecords.size());
        assertEquals(List.of(recA), tableARecords);
    }

    @Test
    void shouldFetchOffsetsFromMetadataWhenExactlyOnceWithTableWriterCreated() throws Exception {
        when(mockSinkConfig.isExactlyOnce()).thenReturn(true);
        // Need to mock internals since metadata service is created in constructor
        mockMetadataService(Map.of(0,10L));

        // Build a fresh service instance so that constructor wires metadata service
        // Use the shared tableWriterMap so that our mocked writer registration is effective
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, tableWriterMap, Map.of(TOPIC_A, Set.of(0)), mockErrorReporter, false, mockTableWriterProvider);

        // Writer created through the provider must receive the offsets fetched from the metadata table
        TableWriter writer = mock(TableWriter.class);
        when(writer.getProcessedPartitionOffsets()).thenReturn(Map.of(0, 10L));
        when(mockTableWriterProvider.get(
                eq(mockSchemaTableA),
                any(),
                any(),
                eq(TOPIC_A),
                eq(Map.of(0, 10L)),
                eq(mockErrorReporter),
                any()
        )).thenReturn(writer);

        // Prepare two records: one below and one above the saved offset (10)
        SinkRecord below = buildRecord(TOPIC_A, 0, 9L);
        SinkRecord above = buildRecord(TOPIC_A, 0, 11L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        assertDoesNotThrow(() -> service.processRecord(List.of(below, above), schemas));

        verify(writer).insertRecords(tableARecordListCaptor.capture());
        assertEquals(1, tableARecordListCaptor.getValue().size());
        assertEquals(List.of(above), tableARecordListCaptor.getValue());
    }

    @Test
    void shouldFilterOutAlreadyProcessedOffsets() throws Exception {
        // Build two records with same topic/partition, second lower offset -> should be filtered
        SinkRecord rec1 = buildRecord(TOPIC_A, 0, 100L);
        SinkRecord rec2 = buildRecord(TOPIC_A, 0, 50L);

        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        // Simulate an existing writer with processed offsets
        TableWriter existingWriter = mock(TableWriter.class);
        Map<Integer, Long> offsets = Map.of(0, 75L);
        when(existingWriter.getProcessedPartitionOffsets()).thenReturn(offsets);

        // Inject existing writer into map
        tableWriterMap.put(TABLE_A, existingWriter);

        assertDoesNotThrow(() -> service.processRecord(List.of(rec1, rec2), schemas));

        // Only rec1 (100L) should be inserted, rec2 (50L) filtered
        verify(existingWriter).insertRecords(tableARecordListCaptor.capture());
        List<SinkRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(1, tableARecords.size());
        assertEquals(List.of(rec1), tableARecords);
    }

    @Test
    void shouldNotCreateWriterForTopicWithoutAssignedPartitions() throws Exception {
        // Topic has a schema but is not assigned to this task instance -> skipped entirely
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, tableWriterMap, Map.of(TOPIC_B, Set.of(0)), mockErrorReporter, false, mockTableWriterProvider);

        SinkRecord recA = buildRecord(TOPIC_A, 0, 1L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        TableWriter writerA = mock(TableWriter.class);
        tableWriterMap.put(TABLE_A, writerA);

        assertDoesNotThrow(() -> service.processRecord(List.of(recA), schemas));
        verify(writerA, never()).insertRecords(anyList());
        verifyNoInteractions(mockTableWriterProvider);
    }

    @Test
    void closeShouldCloseAllTableWriters() {
        TableWriter w1 = mock(TableWriter.class);
        TableWriter w2 = mock(TableWriter.class);
        tableWriterMap.put("t1", w1);
        tableWriterMap.put("t2", w2);

        service.close();
        verify(w1).close();
        verify(w2).close();
    }

    private void mockMetadataService(Map<Integer, Long> partitionOffsets) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(mockDbService.createConnection(any())).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockStatement.executeUpdate(anyString())).thenReturn(1);
        when(mockPreparedStatement.executeQuery()).thenReturn(resultSet);
        AtomicInteger counter = new AtomicInteger(0);
        when(resultSet.next()).thenAnswer(invocation -> counter.incrementAndGet() <= partitionOffsets.size());
        when(resultSet.getInt("topic_partition")).thenReturn(partitionOffsets.keySet().iterator().next());
        when(resultSet.getLong("partition_offset")).thenReturn(partitionOffsets.values().iterator().next());
    }

    private static SinkRecord buildRecord(String topic, int partition, long offset) {
        Schema schema = SchemaBuilder.struct().field("dummy", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        return new SinkRecord(topic, partition, null, null, schema, struct, offset);
    }
}
