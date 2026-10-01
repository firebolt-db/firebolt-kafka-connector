package com.firebolt.kafka.connect.service;

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.SQLException;
import java.util.Collection;

/**
 * We will have a different implementation for append-only (insert) and CDC sink tasks service
 */
public interface FireboltSinkService {

    /**
     * Processes a collection of sink records.
     *
     * @param records the collection of sink records to process
     */
    void processRecord(Collection<SinkRecord> records) throws SQLException;

    /**
     * Closes the resources associated with the firebolt
     */
    void close();

}
