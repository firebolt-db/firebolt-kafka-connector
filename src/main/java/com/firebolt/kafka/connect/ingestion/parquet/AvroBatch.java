package com.firebolt.kafka.connect.ingestion.parquet;

import java.util.List;
import java.util.Map;
import lombok.Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A homogeneous group of records converted to Avro, ready to be written as one Parquet file.
 * {@code fieldToSourceName} maps each (possibly sanitized) Avro field name back to the
 * original record field name, which is what table columns are matched against.
 */
@Value
class AvroBatch {
    Schema schema;
    List<GenericRecord> records;
    Map<String, String> fieldToSourceName;
}
