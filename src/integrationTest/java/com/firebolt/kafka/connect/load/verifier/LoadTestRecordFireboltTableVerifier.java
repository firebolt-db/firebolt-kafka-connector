package com.firebolt.kafka.connect.load.verifier;

import com.firebolt.kafka.connect.clients.FireboltClient;
import com.firebolt.kafka.connect.load.LoadTestRecord;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
/**
 * A verifier checks that the expected records made it to the table.
 */
@Slf4j
public class LoadTestRecordFireboltTableVerifier implements FireboltTableRecordVerifier {

    private List<LoadTestRecord> recordsToVerify = new ArrayList<>();

    @Override
    public boolean verifyRecords(FireboltClient fireboltClient, String tableName) throws SQLException {
        int batchSize = 500;
        List<Integer> nextIds = new ArrayList<>();
        for (int i = 0; i<recordsToVerify.size(); i++) {
            nextIds.add(recordsToVerify.get(i).getColInteger());

            if (nextIds.size() == batchSize) {
                log.info("Verifying a batch of ids: {}", nextIds);
                verifyIds(fireboltClient, tableName, nextIds);

                nextIds = new ArrayList<>();
            }
        }

        if (!nextIds.isEmpty()) {
            log.info("Verifying the last batch");
            verifyIds(fireboltClient, tableName, nextIds);
        }

        return false;
    }

    public void addRecordToVerification(LoadTestRecord loadTestRecord) {
        recordsToVerify.add(loadTestRecord);
    }

    private void verifyIds(FireboltClient client, String tableName, List<Integer> ids) throws SQLException {
        ids = ids.stream().sorted().collect(Collectors.toList()); // natural sorting order is ascending
        StringBuilder sqlStatement = new StringBuilder("select \"colInteger\"")
                .append(" from \"").append(tableName).append("\" ")
                .append(" where \"colInteger\" in (");
        for (int i = 0; i<ids.size() -1; i++) {
            sqlStatement.append(ids.get(i)).append(",");
        }

        // append the last one
        sqlStatement.append(ids.get(ids.size()-1))
                .append(") order by \"colInteger\" asc;");  // order by ids ascending

        ResultSet resultSet = client.executeQuery(sqlStatement.toString());
        List<Integer> actualIds = new ArrayList<>();
        while (resultSet.next()) {
            actualIds.add(resultSet.getInt(1));
        }

        String idsVerified = String.join(",", ids.stream().map(String::valueOf).collect(Collectors.toList()));
        String actualIdsAsString = String.join(",", actualIds.stream().map(String::valueOf).collect(Collectors.toList()));
        assertEquals(ids.size(), actualIds.size(), "Mismatch in ids size. Expected: " + ids.size() + " but was: " + actualIds.size());
        assertEquals(ids, actualIds, "Mismatch in ids. Expected ids: " + idsVerified + ", but was: " + actualIdsAsString);
    }
}
