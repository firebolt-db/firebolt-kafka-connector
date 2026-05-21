package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltMetadataService;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

/**
 * A class that knows how to insert into a Firebolt table . It will use prepared statements to do the inserts.
 * It also tracks what were the last offsets that were written for a particular partition
 */
@Slf4j
public class TableWriter {

    /**
     * For which table this write is for
     */
    private TableSchema tableSchema;

    // when this table writer is created we should fetch the offsets from the metadata table
    private Map<Integer, Long> processedPartitionOffsets;

    private IngestionService ingestionService;
    private FireboltMetadataService fireboltMetadataService;
    private String topicName;

    public TableWriter(TableSchema tableSchema, FireboltMetadataService fireboltMetadataService, String topicName, Map<Integer, Long> processedPartitionOffsets, IngestionService ingestionService) {
        this.tableSchema = tableSchema;
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

        ingestionService.addRecords(fireboltRecords);

        Map<Integer, Long> updatedOffsets = getUpdatedOffsets(fireboltRecords);
        persistProcessedOffsets(updatedOffsets);
        processedPartitionOffsets.putAll(updatedOffsets);
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

    private Map<Integer, Long> getUpdatedOffsets(List<AbstractFireboltRecord> fireboltRecords) {
        Map<Integer, Long> updatedOffsets = new HashMap<>();
        fireboltRecords.forEach(fireboltRecord -> {
            Integer partition = fireboltRecord.getPartition();
            Long offset = fireboltRecord.getOffset();
            Long processedOffset = updatedOffsets.containsKey(partition)
                    ? updatedOffsets.get(partition)
                    : processedPartitionOffsets.get(partition);
            if (processedOffset == null || processedOffset < offset) {
                updatedOffsets.put(partition, offset);
            }
        });
        return updatedOffsets;
    }

    private void persistProcessedOffsets(Map<Integer, Long> updatedOffsets) {
        if (fireboltMetadataService != null && !updatedOffsets.isEmpty()) {
            fireboltMetadataService.updateOffsets(topicName, updatedOffsets);
        }
    }

}
