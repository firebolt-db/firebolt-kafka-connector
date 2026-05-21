package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParquetDataGeneratorTest {

    @Mock
    private ParquetAvroSchemaProvider mockSchemaProvider;
    @Mock
    private AvroNameSanitizer mockAvroNameSanitizer;
    @Mock
    private AvroParquetWriterProvider mockWriterProvider;
    @Mock
    private ParquetWriter<GenericData.Record> mockWriter;
    @Mock
    private ErrorReporter mockErrorReporter;

    @BeforeEach
    void setUpSanitizerIdentity() {
        lenient().when(mockAvroNameSanitizer.toValidAvroName(any())).thenAnswer(inv -> inv.getArgument(0));
    }

    private static class TestWriterProvider extends AvroParquetWriterProvider {
        private final ParquetWriter<GenericData.Record> writer;
        TestWriterProvider(ParquetWriter<GenericData.Record> writer) {
            this.writer = writer;
        }
        @Override
        public ParquetWriter<GenericData.Record> get(Schema avroSchema, org.apache.parquet.io.OutputFile outputFile) {
            return writer;
        }
    }

    @Test
    void returnsEmptyStreamWhenNoRecords() {
        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                new TestWriterProvider(mock(ParquetWriter.class)),
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        OutputStream out = generator.generate(Collections.emptyList(), new TableSchema("t"));
        assertNotNull(out);
        ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
        assertEquals(0, baos.size());

        verifyNoInteractions(mockSchemaProvider, mockWriterProvider);
    }

    @ParameterizedTest
    @CsvSource({
            // tableColumnName, recordAttributeName, avroFieldName
            "TEXT,Text,TEXT",
            "localDate,localdate,localDate",
            "BigInt,BIGINT,BigInt"
    })
    void matchesRecordAttributesCaseInsensitively(String tableColumnName, String recordAttributeName, String avroFieldName) throws Exception {
        TableSchema schema = new TableSchema("t");
        schema.addColumn(tableColumnName, "text", Types.VARCHAR, true);

        Schema avro = SchemaBuilder.record("t")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .name(avroFieldName).type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))).noDefault()
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);

        KafkaMessageColumnValue value = new SchemalessKafkaMessageColumnValue("v");
        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        when(record.getColumnNames()).thenReturn(java.util.Set.of(recordAttributeName));
        when(record.getColumnValue(recordAttributeName)).thenReturn(value);

        doNothing().when(mockWriter).close();

        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                new TestWriterProvider(mockWriter),
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        OutputStream out = generator.generate(List.of(record), schema);
        assertNotNull(out);

        ArgumentCaptor<GenericData.Record> captor = ArgumentCaptor.forClass(GenericData.Record.class);
        verify(mockWriter).write(captor.capture());
        GenericData.Record written = captor.getValue();
        assertEquals("v", written.get(avroFieldName));
    }

    @ParameterizedTest
    @CsvSource({
            "bad-name,bad_name",
            "my$table,my_table",
            "1abc,_1abc",
            "naïve,na_ve"
    })
    void writesValueUnderSanitizedFieldName(String columnName, String avroFieldName) throws Exception {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn(columnName, "text", Types.VARCHAR, true);

        Schema avro = SchemaBuilder.record("orders")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .name(avroFieldName).type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))).noDefault()
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);

        KafkaMessageColumnValue nameValue = new SchemalessKafkaMessageColumnValue("value");
        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        when(record.getColumnNames()).thenReturn(java.util.Set.of(columnName));
        when(record.getColumnValue(columnName)).thenReturn(nameValue);

        when(mockAvroNameSanitizer.toValidAvroName(columnName)).thenReturn(avroFieldName);
        doNothing().when(mockWriter).close();

        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                new TestWriterProvider(mockWriter),
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        OutputStream out = generator.generate(List.of(record), schema);
        assertNotNull(out);

        ArgumentCaptor<GenericData.Record> captor = ArgumentCaptor.forClass(GenericData.Record.class);
        verify(mockWriter).write(captor.capture());
        GenericData.Record written = captor.getValue();
        assertEquals("value", written.get(avroFieldName));
        assertThrows(AvroRuntimeException.class, () -> written.get(columnName));
    }

    @Test
    void convertsValuesAndWritesGenericRecords() throws Exception {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn("id", "integer", Types.INTEGER, false);
        schema.addColumn("name", "text", Types.VARCHAR, true);

        Schema avro = SchemaBuilder.record("orders")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .requiredInt("id")
                .name("name").type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))).noDefault()
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);

        KafkaMessageColumnValue idValue = new SchemalessKafkaMessageColumnValue(123);
        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        when(record.getColumnNames()).thenReturn(java.util.Set.of("id", "name"));
        when(record.getColumnValue("id")).thenReturn(idValue);
        when(record.getColumnValue("name")).thenReturn(null);

        doNothing().when(mockWriter).close();
        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                new TestWriterProvider(mockWriter),
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        OutputStream out = generator.generate(List.of(record), schema);
        assertNotNull(out);

        ArgumentCaptor<GenericData.Record> captor = ArgumentCaptor.forClass(GenericData.Record.class);
        verify(mockWriter).write(captor.capture());
        GenericData.Record written = captor.getValue();
        assertEquals(123, written.get("id"));
        assertEquals(null, written.get("name"));
    }

    @Test
    void wrapsWriterExceptions() throws Exception {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn("id", "integer", Types.INTEGER, false);

        Schema avro = SchemaBuilder.record("orders")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .requiredInt("id")
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);
        doNothing().when(mockWriter).close();

        KafkaMessageColumnValue idValue = new SchemalessKafkaMessageColumnValue(123);
        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        when(record.getColumnNames()).thenReturn(java.util.Set.of("id"));
        when(record.getColumnValue("id")).thenReturn(idValue);

        org.mockito.Mockito.doThrow(new IOException("disk full")).when(mockWriter).write(any(GenericData.Record.class));

        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                new TestWriterProvider(mockWriter),
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        assertThrows(RuntimeException.class, () -> generator.generate(List.of(record), schema));
    }

    @Test
    void skipsRecordsThatFailColumnConversion() throws Exception {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn("id", "integer", Types.INTEGER, false);

        Schema avro = SchemaBuilder.record("orders")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .requiredInt("id")
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);
        doNothing().when(mockWriter).close();

        // Good record
        KafkaMessageColumnValue idValue1 = new SchemalessKafkaMessageColumnValue(1);
        AbstractFireboltRecord good = mock(AbstractFireboltRecord.class);
        when(good.getColumnNames()).thenReturn(java.util.Set.of("id"));
        when(good.getColumnValue("id")).thenReturn(idValue1);

        // Bad record - converter throws ColumnConversionFailedException
        KafkaMessageColumnValue idValue2 = new SchemalessKafkaMessageColumnValue("bad");
        AbstractFireboltRecord bad = mock(AbstractFireboltRecord.class);
        when(bad.getColumnNames()).thenReturn(java.util.Set.of("id"));
        when(bad.getColumnValue("id")).thenReturn(idValue2);

        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                new TestWriterProvider(mockWriter),
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        OutputStream out = generator.generate(List.of(good, bad), schema);
        assertNotNull(out);

        // Only the good record is written
        ArgumentCaptor<GenericData.Record> captor = ArgumentCaptor.forClass(GenericData.Record.class);
        verify(mockWriter).write(captor.capture());
        GenericData.Record written = captor.getValue();
        assertEquals(1, written.get("id"));

        // Error reported for the bad record
        verify(mockErrorReporter).report(any(), any(Exception.class));
    }

    @Test
    void wrapsWriterProviderGetExceptions() throws Exception {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn("id", "integer", Types.INTEGER, false);

        Schema avro = SchemaBuilder.record("orders")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .requiredInt("id")
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);
        // Throw when obtaining the writer (inside try-with-resources)
        when(mockWriterProvider.get(eq(avro), any())).thenThrow(new IOException("create failed"));

        KafkaMessageColumnValue idValue = new SchemalessKafkaMessageColumnValue(123);
        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        when(record.getColumnNames()).thenReturn(java.util.Set.of("id"));
        when(record.getColumnValue("id")).thenReturn(idValue);

        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                mockWriterProvider,
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                true
        );

        RuntimeException ex = assertThrows(RuntimeException.class, () -> generator.generate(List.of(record), schema));
        assertTrue(ex.getMessage().startsWith("Failed to write Parquet content in-memory"));
        assertNotNull(ex.getCause());
        assertEquals(IOException.class, ex.getCause().getClass());
    }

    @Test
    void throwsOnColumnConversionWhenErrorToleranceDisabled() throws Exception {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn("id", "integer", Types.INTEGER, false);

        Schema avro = SchemaBuilder.record("orders")
                .namespace("com.firebolt.kafka.connect")
                .fields()
                .requiredInt("id")
                .endRecord();

        when(mockSchemaProvider.get(schema)).thenReturn(avro);
        when(mockWriterProvider.get(eq(avro), any())).thenReturn(mockWriter);
        doNothing().when(mockWriter).close();

        KafkaMessageColumnValue idValue = new SchemalessKafkaMessageColumnValue("bad");
        AbstractFireboltRecord bad = mock(AbstractFireboltRecord.class);
        when(bad.getColumnNames()).thenReturn(java.util.Set.of("id"));
        when(bad.getColumnValue("id")).thenReturn(idValue);

        ParquetDataGenerator generator = new ParquetDataGenerator(
                mockSchemaProvider,
                mockWriterProvider,
                mockAvroNameSanitizer,
                new ParquetDataGenerator.InMemoryFileProvider(),
                mockErrorReporter,
                false
        );

        assertThrows(RecordConversionFailedException.class, () -> generator.generate(List.of(bad), schema));
        verify(mockWriter, never()).write(any(GenericData.Record.class));
    }
}
