package com.firebolt.kafka.connect.e2e;

import com.firebolt.kafka.connect.clients.FireboltClient;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates E2E test data that has landed in Firebolt.
 * Provides row-count checks, data integrity spot-checks,
 * and checksum validation for chaos/exactly-once testing.
 */
@Slf4j
public class FireboltValidator {

    private final FireboltClient fireboltClient;

    public FireboltValidator(FireboltClient fireboltClient) {
        this.fireboltClient = fireboltClient;
    }

    /**
     * Asserts the table contains exactly the expected number of rows.
     */
    public void validateRecordCount(String tableName, int expectedCount) throws SQLException {
        int actual = fireboltClient.countRows(tableName);
        log.info("Table '{}' row count: expected={}, actual={}", tableName, expectedCount, actual);
        assertEquals(expectedCount, actual,
                "Row count mismatch in table '" + tableName + "'");
    }

    /**
     * Spot-checks data integrity by verifying a few known records.
     * Checks that sequential IDs 1, N/2, and N exist with correct name prefix.
     */
    public void validateDataIntegrity(String tableName, int totalRecords) throws SQLException {
        long[] sampleIds = {1, totalRecords / 2, totalRecords};
        for (long sampleId : sampleIds) {
            validateRecordExists(tableName, sampleId);
        }
        log.info("Data integrity check passed for table '{}' ({} spot-checks)",
                tableName, sampleIds.length);
    }

    /**
     * Validates exactly-once delivery via checksum comparison.
     * Compares checksum of the id column against generate_series(1, n).
     * Used by chaos tests (PR 3), but built here for reuse.
     */
    public void validateChecksum(String tableName, String column, int n) throws SQLException {
        // Explicit BIGINT cast ensures consistent 8-byte checksumming
        String actualSql = String.format(
                "SELECT checksum(\"%s\"::BIGINT) FROM \"%s\"", column, tableName);
        String expectedSql = String.format(
                "SELECT checksum(x::BIGINT) FROM generate_series(1, %d) r(x)", n);

        long actualChecksum;
        try (ResultSet actualRs = fireboltClient.executeQuery(actualSql);
             Statement actualStmt = actualRs.getStatement()) {
            assertTrue(actualRs.next(), "No checksum result from table");
            actualChecksum = actualRs.getLong(1);
        }

        long expectedChecksum;
        try (ResultSet expectedRs = fireboltClient.executeQuery(expectedSql);
             Statement expectedStmt = expectedRs.getStatement()) {
            assertTrue(expectedRs.next(), "No checksum result from generate_series");
            expectedChecksum = expectedRs.getLong(1);
        }

        log.info("Checksum validation: table={}, column={}, n={}, expected={}, actual={}",
                tableName, column, n, expectedChecksum, actualChecksum);
        assertEquals(expectedChecksum, actualChecksum,
                "Checksum mismatch — data integrity violation in '" + tableName + "'");
    }

    /**
     * Validates that a specific record exists by ID with the expected name prefix.
     */
    private void validateRecordExists(String tableName, long id) throws SQLException {
        String sql = String.format(
                "SELECT \"id\", \"name\" FROM \"%s\" WHERE \"id\" = %d", tableName, id);
        try (ResultSet rs = fireboltClient.executeQuery(sql);
             Statement stmt = rs.getStatement()) {
            assertTrue(rs.next(), "Record id=" + id + " not found in table '" + tableName + "'");
            String name = rs.getString("name");
            assertTrue(name.startsWith("record-" + id),
                    "Record id=" + id + " has unexpected name: " + name);
        }
    }
}
