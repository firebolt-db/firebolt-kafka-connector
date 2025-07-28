package com.firebolt.kafka.connect.service;

import com.firebolt.kafka.connect.TableSchema;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * We will have a different implementation for append-only (insert) and CDC sink tasks service
 */
public interface FireboltSinkService {

    /**
     * Processes a collection of sink records with the provided table schema context.
     *
     * @param records the collection of sink records to process
     * @param tableSchemas the map of table names to their schemas for context
     */
    void processRecord(Collection<SinkRecord> records, Map<String, TableSchema> tableSchemas);

}