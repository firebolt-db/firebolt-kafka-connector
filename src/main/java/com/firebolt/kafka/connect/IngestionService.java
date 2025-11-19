package com.firebolt.kafka.connect;

import java.sql.SQLException;
import java.util.List;

/**
 * A service that will know to ingest the firebolt records to the firebolt db
 */
public interface IngestionService {

    /**
     * @param fireboltRecords - the records to be inserted
     * @throws SQLException - an exception in case there is a problem talking to the firbolt db
     */
    void addRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException;

}
