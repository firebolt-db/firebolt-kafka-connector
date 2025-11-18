package com.firebolt.kafka.connect.ingestion.binary.parquet;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

public class AvroParquetWriterProvider {

    public ParquetWriter<GenericData.Record> get(Schema avroSchema, OutputFile outputFile) throws IOException {
        return AvroParquetWriter.<GenericData.Record>builder(outputFile)
                .withSchema(avroSchema)
                .build();
    }
}
