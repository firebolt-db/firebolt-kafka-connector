package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltMetadataService;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

/**
 * A class that knows how to insert into a Firebolt table. It will use prepared statements to do the inserts.
 * It also tracks what were the last offsets that were written for a particular partition.
 *
 * When exactly-once delivery is enabled (fireboltMetadataService is non-null), each batch is wrapped
 * in a single Firebolt transaction: BEGIN; INSERT data; UPDATE KafkaSinkConnectorMetadata; COMMIT.
 * This guarantees that committed offsets in the metadata table are always in sync with committed data.
 */
@Slf4j
public class TableWriter {

    /**
     * For which table this write is for
     */
    private TableSchema tableSchema;

    // Shared JDBC connection — used directly for transaction control in exactly-once mode
    private Connection connection;

    // when this table writer is created we should fetch the offsets from the metadata table
    private Map<Integer, Long> processedPartitionOffsets;

    private IngestionService ingestionService;

    // Non-null only in exactly-once mode
    private FireboltMetadataService fireboltMetadataService;
    private String topicName;

    public TableWriter(TableSchema tableSchema, Connection connection, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, IngestionService ingestionService) {
        this.tableSchema = tableSchema;
        this.connection = connection;
        this.fireboltMetadataService = fireboltMetadataService;
        this.topicName = topicName;
        this.processedPartitionOffsets = processedPartitionOffsets;
        this.ingestionService = ingestionService;
    }

    public void insertRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        log.debug("Processing {} records for table: {}", fireboltRecords.size(), tableSchema.getTableName());

        if (CollectionUtils.isEmpty(fireboltRecords)) {
            return;
        }

        if (fireboltMetadataService != null) {
            // Exactly-once: wrap the data insert and the offset update in a single Firebolt transaction
            // so they are always in sync — a crash can never leave one committed without the other.
            connection.setAutoCommit(false);
            try {
                ingestionService.addRecords(fireboltRecords);
                fireboltMetadataService.updateOffsets(connection, topicName, computeMaxOffsets(fireboltRecords));
                connection.commit();
            } catch (SQLException e) {
                log.error("Exactly-once ingestion failed, rolling back transaction", e);
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } else {
            ingestionService.addRecords(fireboltRecords);
        }

        updateProcessedOffsets(fireboltRecords);
    }

    public Map<Integer, Long> getProcessedPartitionOffsets() {
        return Collections.unmodifiableMap(processedPartitionOffsets);
    }

    public void close() {
        if (ingestionService != null) {
            try {
                ingestionService.close();
            } catch (Exception e) {
                log.error("Failed to gracefully close the ingestion service");
            }
        }
    }

    /**
     * Returns the highest offset seen per partition across the given records.
     * Only partitions present in this batch are included — unchanged partitions retain their
     * existing value in the metadata table and do not need a redundant UPDATE.
     */
    private Map<Integer, Long> computeMaxOffsets(List<AbstractFireboltRecord> records) {
        Map<Integer, Long> newOffsets = new HashMap<>();
        for (AbstractFireboltRecord record : records) {
            newOffsets.merge(record.getPartition(), record.getOffset(), Math::max);
        }
        return newOffsets;
    }

    private void updateProcessedOffsets(List<AbstractFireboltRecord> fireboltRecords) {
        fireboltRecords.forEach(fireboltRecord -> {
            Integer partition = fireboltRecord.getPartition();
            Long offset = fireboltRecord.getOffset();
            if (processedPartitionOffsets.get(partition) < offset) {
                processedPartitionOffsets.put(partition, offset);
            }
        });
    }

}
