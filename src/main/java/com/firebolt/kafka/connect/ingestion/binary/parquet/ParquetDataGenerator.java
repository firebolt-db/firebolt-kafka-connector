package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.BinaryDataGenerator;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * The outputStream will be in the parquet format.
 */
@Slf4j
public class ParquetDataGenerator implements BinaryDataGenerator {

    private ParquetAvroSchemaProvider parquetAvroSchemaProvider;
    private ColumnDataTypeConverterFactory columnDataTypeConverterFactory;
    private AvroParquetWriterProvider avroParquetWriterProvider;
    private InMemoryFileProvider inMemoryFileProvider;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;
    private AvroNameSanitizer avroNameSanitizer ;

    public ParquetDataGenerator(ErrorReporter errorReporter, boolean errorToleranceAll) {
        this(new ParquetAvroSchemaProvider(),
                ColumnDataTypeFactoryProvider.getInstance(),
                new AvroParquetWriterProvider(),
                new AvroNameSanitizer(),
                new InMemoryFileProvider(),
                errorReporter, errorToleranceAll
        );
    }

    @VisibleForTesting
    ParquetDataGenerator(ParquetAvroSchemaProvider parquetAvroSchemaProvider,
                         ColumnDataTypeConverterFactory columnDataTypeConverterFactory,
                         AvroParquetWriterProvider avroParquetWriterProvider,
                         AvroNameSanitizer avroNameSanitizer,
                         InMemoryFileProvider inMemoryFileProvider,
                         ErrorReporter errorReporter,
                         boolean errorToleranceAll) {
        this.parquetAvroSchemaProvider = parquetAvroSchemaProvider;
        this.columnDataTypeConverterFactory = columnDataTypeConverterFactory;
        this.avroParquetWriterProvider = avroParquetWriterProvider;
        this.avroNameSanitizer = avroNameSanitizer;
        this.inMemoryFileProvider = inMemoryFileProvider;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
    }

    /**
     * Generates a parquet output stream from the records that we got from Kafka, based on the table schema where these
     * values will be inserted to
     * @param records - the records from the kafka topic
     * @param tableSchema - the table schema of the table where the data will be inserted. Must be non null
     * @return
     */
    @Override
    public OutputStream generate(List<AbstractFireboltRecord> records, TableSchema tableSchema) {
        if (records == null || records.isEmpty()) {
            return new ByteArrayOutputStream(0);
        }

        Schema avroSchema = parquetAvroSchemaProvider.get(tableSchema);

        List<GenericData.Record> avroRecords = processRecords(records, tableSchema, avroSchema);

        InMemoryOutputFile out = inMemoryFileProvider.get();
        try (ParquetWriter<GenericData.Record> writer = avroParquetWriterProvider.get(avroSchema, out)) {
            for (GenericData.Record avroRecord : avroRecords) {
                writer.write(avroRecord);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write Parquet content in-memory", e);
        }

        return out.getBuffer();
    }

    private List<GenericData.Record> processRecords(List<AbstractFireboltRecord> records, TableSchema tableSchema, Schema avroSchema) {
        List<GenericData.Record> avroRecords = new ArrayList<>();
        for (AbstractFireboltRecord record : records) {
            try {
                GenericData.Record processedRecords = processRecord(record, tableSchema, avroSchema);
                avroRecords.add(processedRecords);
            } catch (RecordConversionFailedException e) {
                if (errorToleranceAll) {
                    errorReporter.report(record.getSinkRecord(), e);
                    log.warn("Record from partition {} at offset {} will be submitted to the deadletter queue ", e.getKafkaPartition(), e.getKafkaOffset());
                } else {
                    throw e;
                }
            }
        }
        return avroRecords;
    }

    private GenericData.Record processRecord(AbstractFireboltRecord record, TableSchema tableSchema, Schema avroSchema) {
        GenericData.Record avroRecord = new GenericData.Record(avroSchema);
        for (TableSchema.Column column : tableSchema.getColumns()) {
            String columnName = column.getName();
            KafkaMessageColumnValue kafkaMessageColumnValue = record.getColumnValue(columnName);

            if (kafkaMessageColumnValue == null || kafkaMessageColumnValue.getValue()== null) {
                avroRecord.put(columnName, null);
                continue;
            }

            try {
                Object convertedValue = columnDataTypeConverterFactory.getConverter(column).toParquetValue(kafkaMessageColumnValue, column);
                avroRecord.put(avroNameSanitizer.toValidAvroName(columnName), convertedValue);
            } catch (ColumnConversionFailedException e) {
                // as of now we are failing at the first column conversion failure. We could try to convert all the columns so we give all the data in one record convertion exception.
                throw RecordConversionFailedException.builder()
                        .message(e.getMessage())
                        .tableName(tableSchema.getTableName())
                        .kafkaPartition(record.getPartition())
                        .kafkaOffset(record.getOffset())
                        .topicName(record.getTopic())
                        .build();
            }
        }

        return avroRecord;
    }

    private static final class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new BAOSPositionOutputStream(buffer);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            buffer.reset();
            return new BAOSPositionOutputStream(buffer);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        ByteArrayOutputStream getBuffer() {
            return buffer;
        }
    }

    private static final class BAOSPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream delegate;

        BAOSPositionOutputStream(ByteArrayOutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return delegate.size();
        }

        @Override
        public void write(int b) {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            delegate.write(b, off, len);
        }

        @Override
        public void close() {
            // Do not close the underlying buffer; allow caller to read it
        }
    }

    public static class InMemoryFileProvider {
        InMemoryOutputFile get() {
            return new InMemoryOutputFile();
        }
    }
}
