package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.TableWriter;
import com.firebolt.kafka.connect.convert.RecordConverterFactory;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, errorReporter, false);

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
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, errorReporter, true);
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
        verifyNoInteractions(mockSinkConfig, mockDbService, mockConverterFactory);
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
        service = new AppendOnlyFireboltSinkService(mockSinkConfig, mockDbService, mockConverterFactory, tableWriterMap, errorReporter, true);
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

    private static SinkRecord buildRecord(String topic, int partition, long offset) {
        Schema schema = SchemaBuilder.struct().field("dummy", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        return new SinkRecord(topic, partition, null, null, schema, struct, offset);
    }
}


