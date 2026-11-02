package com.firebolt.kafka.connect;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A service that will know to ingest the kafka records to the firebolt db
 */
public interface IngestionService {

    /**
     * @param records        - the records to be inserted
     * @param literalColumns - extra column values (e.g. a batch id) inserted as constants
     *                       alongside the record data, when the target table has the column
     * @throws SQLException - an exception in case there is a problem talking to the firebolt db
     */
    void addRecords(List<SinkRecord> records, Map<String, String> literalColumns) throws SQLException;

    default void addRecords(List<SinkRecord> records) throws SQLException {
        addRecords(records, Map.of());
    }

    /**
     * Close all resources associated with the ingestion service
     */
    void close() throws Exception;
}
