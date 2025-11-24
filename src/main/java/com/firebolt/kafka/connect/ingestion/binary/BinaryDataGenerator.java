package com.firebolt.kafka.connect.ingestion.binary;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.TableSchema;
import java.io.OutputStream;
import java.util.List;

public interface BinaryDataGenerator {

    /**
     * Generates the records to be inserted into the table schema in a binary format.
     *
     * @param records - the records from the kafka topic
     * @param tableSchema - the table schema of the table where the data will be inserted
     * @return - an output stream of the data that will be inserted into the table
     */
    OutputStream generate(List<AbstractFireboltRecord> records, TableSchema tableSchema);

}
