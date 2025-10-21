package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.TableWriter;
import com.firebolt.kafka.connect.convert.RecordConverterFactory;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;

import java.lang.reflect.Field;
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

import com.firebolt.kafka.connect.reporter.ErrorReporter;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
    private RecordConverterFactory mockConverterFactory;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Captor
    private ArgumentCaptor<List> tableARecordListCaptor;

    @Captor
    private ArgumentCaptor<List> tableBRecordListCaptor;

    private AppendOnlyFireboltSinkService service;
    private Map<String, TableWriter> tableWriterMap;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        tableWriterMap = new HashMap<>();
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, Map.of(TOPIC_A, Set.of(0), TOPIC_B, Set.of(0)), errorReporter, false);

        when(mockSinkConfig.isExactlyOnce()).thenReturn(false);
        when(mockSinkConfig.getTableNameForTopic(TOPIC_A)).thenReturn(TABLE_A);
        when(mockSinkConfig.getTableNameForTopic(TOPIC_B)).thenReturn(TABLE_B);
        when(mockSinkConfig.getJdbcConfig()).thenReturn(null);

        when(mockDbService.createConnection(any())).thenReturn(mockConnection);

        when(mockSchemaTableA.getTableName()).thenReturn(TABLE_A);
        when(mockSchemaTableB.getTableName()).thenReturn(TABLE_B);
    }

    @Test
    void shouldReportConversionFailureViaErrorReporter() throws Exception {
        //rebuild service with error tolerance enabled
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, Map.of(TOPIC_A, Set.of(0), TOPIC_B, Set.of(0)), errorReporter, true);
        SinkRecord badRecord = buildRecord(TOPIC_A, 0, 1L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        TableWriter writerA = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);

        when(mockConverterFactory.convert(badRecord)).thenThrow(new RecordConversionException("boom"));

        assertDoesNotThrow(() -> service.processRecord(java.util.List.of(badRecord), schemas));

        verify(errorReporter, times(1)).report(eq(badRecord), any(Exception.class));
    }

    @Test
    void shouldNotReportWhenConversionSucceeds() throws Exception {
        SinkRecord ok = buildRecord(TOPIC_A, 0, 2L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        TableWriter writerA = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);

        FireboltRecord converted = mock(FireboltRecord.class);
        when(mockConverterFactory.convert(ok)).thenReturn(converted);

        assertDoesNotThrow(() -> service.processRecord(java.util.List.of(ok), schemas));

        verifyNoInteractions(errorReporter);
        verify(writerA, times(1)).insertRecords(anyList());
    }

    @Test
    void shouldReturnWhenNoRecords() {
        assertDoesNotThrow(() -> service.processRecord(List.of(), Map.of()));
        verify(mockSinkConfig, times(1)).isExactlyOnce();
        verifyNoMoreInteractions(mockSinkConfig);
        verifyNoInteractions(mockDbService, mockConverterFactory);
    }

    @Test
    void shouldIgnoreRecordsWhenTableSchemaMissing()  {
        SinkRecord rec = buildRecord(TOPIC_A, 0, 1L);
        Map<String, TableSchema> schemas = Map.of();

        // Converter shouldn't be called as schema missing -> writer not created
        assertDoesNotThrow(() -> service.processRecord(List.of(rec), schemas));
        verifyNoInteractions(mockConverterFactory);
    }

    @Test
    void shouldGroupByTopicAndInsertConvertedRecords() throws Exception {
        // two topics
        SinkRecord recA1 = buildRecord(TOPIC_A, 0, 1L);
        SinkRecord recA2 = buildRecord(TOPIC_A, 0, 2L);
        SinkRecord recB1 = buildRecord(TOPIC_B, 0, 5L);

        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA, TABLE_B, mockSchemaTableB);

        // mock conversion
        FireboltRecord convertedA1 = mock(FireboltRecord.class);
        FireboltRecord convertedA2 = mock(FireboltRecord.class);
        FireboltRecord convertedB1 = mock(FireboltRecord.class);
        when(mockConverterFactory.convert(recA1)).thenReturn(convertedA1);
        when(mockConverterFactory.convert(recA2)).thenReturn(convertedA2);
        when(mockConverterFactory.convert(recB1)).thenReturn(convertedB1);

        // Provide pre-created writers to avoid DB work and to verify inserts per table
        TableWriter writerA = mock(TableWriter.class);
        TableWriter writerB = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        when(writerB.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);
        tableWriterMap.put(TABLE_B, writerB);

        // process
        assertDoesNotThrow(() -> service.processRecord(List.of(recA1, recA2, recB1), schemas));
        verify(mockConverterFactory, times(3)).convert(any(SinkRecord.class));
        verify(writerA).insertRecords(tableARecordListCaptor.capture());
        verify(writerB).insertRecords(tableBRecordListCaptor.capture());

        List<FireboltRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(2, tableARecords.size());
        assertEquals(List.of(convertedA1, convertedA2), tableARecords);

        List<FireboltRecord> tableBRecords = tableBRecordListCaptor.getValue();
        assertEquals(1, tableBRecords.size());
        assertEquals(List.of(convertedB1), tableBRecords);
    }

    @Test
    void shouldUseDefaultOffsetsWhenNotExactlyOnce() throws Exception {
        when(mockSinkConfig.isExactlyOnce()).thenReturn(false);

        // Pre-create writer to intercept offsets passed to processRecordsForTopic
        TableWriter writerA = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);

        SinkRecord recA = buildRecord(TOPIC_A, 0, 1L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        FireboltRecord convertedA = mock(FireboltRecord.class);
        when(mockConverterFactory.convert(recA)).thenReturn(convertedA);

        assertDoesNotThrow(() -> service.processRecord(List.of(recA), schemas));

        // since not exactly once, offsets for assigned partition 0 should default to -1
        verify(writerA).insertRecords(tableARecordListCaptor.capture());
        List<FireboltRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(1, tableARecords.size());
        assertEquals(List.of(convertedA), tableARecords);
    }

    @Test
    void shouldFetchOffsetsFromMetadataWhenExactlyOnceWithTableWriterCreated() throws Exception {
        when(mockSinkConfig.isExactlyOnce()).thenReturn(true);
        // Need to mock internals since metadata service is created in constructor
        mockMetadataService(Map.of(0,10L));

        // Build a fresh service instance so that constructor wires metadata service
        // Use the shared tableWriterMap so that our mocked writer registration is effective
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, Map.of(TOPIC_A, Set.of(0)), errorReporter, false);

        // Inject a mock FireboltMetadataService and stub its behavior
        Field f = AppendOnlyFireboltSinkService.class.getDeclaredField("fireboltMetadataService");
        f.setAccessible(true);
        FireboltMetadataService real = (FireboltMetadataService) f.get(service);
        FireboltMetadataService spyMetadata = spy(real);
        f.set(service, spyMetadata);

        TableWriter writerA = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of(0, 10L));
        doNothing().when(writerA).insertRecords(anyList());
        tableWriterMap.put(TABLE_A, writerA);

        // Prepare two records: one below and one above the saved offset (10)
        SinkRecord below = buildRecord(TOPIC_A, 0, 9L);
        SinkRecord above = buildRecord(TOPIC_A, 0, 11L);
        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        FireboltRecord convertedBelow = mock(FireboltRecord.class);
        FireboltRecord convertedAbove = mock(FireboltRecord.class);
        when(mockConverterFactory.convert(below)).thenReturn(convertedBelow);
        when(mockConverterFactory.convert(above)).thenReturn(convertedAbove);

        assertDoesNotThrow(() -> service.processRecord(List.of(below, above), schemas));
        verify(spyMetadata, times(0)).getLastOffsets(TOPIC_A, Set.of(0));
        verify(mockConverterFactory, times(1)).convert(any(SinkRecord.class));
        verify(writerA).insertRecords(tableARecordListCaptor.capture());
        assertEquals(1, tableARecordListCaptor.getValue().size());
        assertEquals(List.of(convertedAbove), tableARecordListCaptor.getValue());
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

        // Only rec1 (100L) should be converted, rec2 (50L) filtered
        FireboltRecord converted1 = mock(FireboltRecord.class);
        when(mockConverterFactory.convert(rec1)).thenReturn(converted1);

        assertDoesNotThrow(() -> service.processRecord(List.of(rec1, rec2), schemas));
        verify(mockConverterFactory).convert(any(SinkRecord.class));

        verify(existingWriter).insertRecords(tableARecordListCaptor.capture());
        List<FireboltRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(1, tableARecords.size());
        assertEquals(List.of(converted1), tableARecords);
    }

    @Test
    void shouldFilterOutRecordsThatCannotBeConvertedWhenErrorToleranceIsAll() throws Exception {
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, Map.of(TOPIC_A, Set.of(0), TOPIC_B, Set.of(0)), errorReporter, true);
        // two topics
        SinkRecord recA1 = buildRecord(TOPIC_A, 0, 1L);
        SinkRecord recA2 = buildRecord(TOPIC_A, 0, 2L);
        SinkRecord recB1 = buildRecord(TOPIC_B, 0, 5L);

        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA, TABLE_B, mockSchemaTableB);

        // mock conversion
        FireboltRecord convertedA2 = mock(FireboltRecord.class);
        FireboltRecord convertedB1 = mock(FireboltRecord.class);
        when(mockConverterFactory.convert(recA1)).thenThrow(new RecordConversionException("failing"));
        when(mockConverterFactory.convert(recA2)).thenReturn(convertedA2);
        when(mockConverterFactory.convert(recB1)).thenReturn(convertedB1);

        // Provide pre-created writers to avoid DB work and to verify inserts per table
        TableWriter writerA = mock(TableWriter.class);
        TableWriter writerB = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        when(writerB.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);
        tableWriterMap.put(TABLE_B, writerB);

        // process
        assertDoesNotThrow(() -> service.processRecord(List.of(recA1, recA2, recB1), schemas));
        verify(mockConverterFactory, times(3)).convert(any(SinkRecord.class));
        verify(writerA).insertRecords(tableARecordListCaptor.capture());
        verify(writerB).insertRecords(tableBRecordListCaptor.capture());

        List<FireboltRecord> tableARecords = tableARecordListCaptor.getValue();
        assertEquals(1, tableARecords.size());
        assertEquals(List.of(convertedA2), tableARecords);

        List<FireboltRecord> tableBRecords = tableBRecordListCaptor.getValue();
        assertEquals(1, tableBRecords.size());
        assertEquals(List.of(convertedB1), tableBRecords);
    }

    @Test
    void shouldThrowWhenRecordCannotBeConvertedWithErrorToleranceIsNone() throws Exception {
        SinkRecord recA1 = buildRecord(TOPIC_A, 0, 1L);
        SinkRecord recA2 = buildRecord(TOPIC_A, 0, 2L);

        Map<String, TableSchema> schemas = Map.of(TABLE_A, mockSchemaTableA);

        when(mockConverterFactory.convert(recA1)).thenThrow(new RecordConversionException("failing"));

        TableWriter writerA = mock(TableWriter.class);
        when(writerA.getProcessedPartitionOffsets()).thenReturn(Map.of());
        tableWriterMap.put(TABLE_A, writerA);

        assertThrows(RecordConversionException.class,
                () -> service.processRecord(List.of(recA1, recA2), schemas));

        verify(mockConverterFactory).convert(recA1);
        verifyNoMoreInteractions(mockConverterFactory);

        verify(writerA).getProcessedPartitionOffsets();
        verifyNoMoreInteractions(writerA);
        verifyNoInteractions(errorReporter);
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


