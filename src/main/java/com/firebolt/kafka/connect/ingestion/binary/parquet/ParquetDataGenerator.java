package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.BinaryDataGenerator;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * The outputStream will be in the parquet format.
 */
@Slf4j
public class ParquetDataGenerator implements BinaryDataGenerator {

    private ParquetAvroSchemaProvider parquetAvroSchemaProvider;
    private AvroParquetWriterProvider avroParquetWriterProvider;
    private InMemoryFileProvider inMemoryFileProvider;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;
    private AvroNameSanitizer avroNameSanitizer ;

    public ParquetDataGenerator(ErrorReporter errorReporter, boolean errorToleranceAll) {
        this(new ParquetAvroSchemaProvider(),
                new AvroParquetWriterProvider(),
                new AvroNameSanitizer(),
                new InMemoryFileProvider(),
                errorReporter, errorToleranceAll
        );
    }

    @VisibleForTesting
    ParquetDataGenerator(ParquetAvroSchemaProvider parquetAvroSchemaProvider,
                         AvroParquetWriterProvider avroParquetWriterProvider,
                         AvroNameSanitizer avroNameSanitizer,
                         InMemoryFileProvider inMemoryFileProvider,
                         ErrorReporter errorReporter,
                         boolean errorToleranceAll) {
        this.parquetAvroSchemaProvider = parquetAvroSchemaProvider;
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

        InMemoryOutputFile out = inMemoryFileProvider.get();
        try (ParquetWriter<GenericData.Record> writer = avroParquetWriterProvider.get(avroSchema, out)) {
            writeRecords(records, tableSchema, avroSchema, writer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write Parquet content in-memory", e);
        }

        return out.getBuffer();
    }

    private void writeRecords(List<AbstractFireboltRecord> records,
                              TableSchema tableSchema,
                              Schema avroSchema,
                              ParquetWriter<GenericData.Record> writer) throws java.io.IOException {
        for (AbstractFireboltRecord record : records) {
            try {
                writer.write(processRecord(record, tableSchema, avroSchema));
            } catch (RecordConversionFailedException e) {
                if (errorToleranceAll) {
                    errorReporter.report(record.getSinkRecord(), e);
                    log.warn("Record from partition {} at offset {} will be submitted to the deadletter queue ", e.getKafkaPartition(), e.getKafkaOffset());
                } else {
                    throw e;
                }
            }
        }
    }

    private GenericData.Record processRecord(AbstractFireboltRecord record, TableSchema tableSchema, Schema avroSchema) {
        GenericData.Record avroRecord = new GenericData.Record(avroSchema);

        // key is the lower case record name columns and value is the actual column name
        Map<String,String> recordAttributeNames = record.getColumnNames().stream().collect(Collectors.toMap(name -> name.toLowerCase(), Function.identity()));

        for (TableSchema.Column column : tableSchema.getColumns()) {
            String tableColumnName = column.getName();

            // look up the column names using case insensitive search
            String recordAttributeName = recordAttributeNames.get(tableColumnName.toLowerCase());

            // only process the attributes from the record that match a column name in the table
            if (StringUtils.isBlank(recordAttributeName)) {
                continue;
            }

            KafkaMessageColumnValue kafkaMessageColumnValue = record.getColumnValue(recordAttributeName);

            if (kafkaMessageColumnValue == null || kafkaMessageColumnValue.getValue()== null) {
                avroRecord.put(tableColumnName, null);
                continue;
            }

            try {
                boolean hasSchema = kafkaMessageColumnValue instanceof SchemaKafkaMessageColumnValue;
                Object convertedValue = BinaryColumnDataTypeFactoryProvider.getInstance(hasSchema).getConverter(column).toParquetValue(kafkaMessageColumnValue, column);

                // avro schema is using the column name from the table schema
                avroRecord.put(avroNameSanitizer.toValidAvroName(tableColumnName), convertedValue);
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
