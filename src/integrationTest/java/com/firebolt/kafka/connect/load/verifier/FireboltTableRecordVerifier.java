package com.firebolt.kafka.connect.load.verifier;

import com.firebolt.kafka.connect.clients.FireboltClient;
import java.sql.SQLException;

/**
 * Makes sure that the records from the table actually matches the generated records
 */
public interface FireboltTableRecordVerifier {

    /**
     * Verifies that the expected records made it to the firebolt table
     * @param fireboltClient - the client that can be used to interact with the firebolt table
     * @param tableName - the table that should contain the records that we are expecting
     * @return
     */
    boolean verifyRecords(FireboltClient fireboltClient, String tableName) throws SQLException;

}
