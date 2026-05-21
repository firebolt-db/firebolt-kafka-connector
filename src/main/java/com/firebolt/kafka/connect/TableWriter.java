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
 * A class that knows how to insert into a Firebolt table. It will use prepared statements to do the inserts.
 * It also tracks what were the last offsets that were written for a particular partition.
 */
@Slf4j
public class TableWriter {

    /**
     * For which table this write is for
     */
    private TableSchema tableSchema;

    // Last accepted offsets by Kafka partition.
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

        Map<Integer, Long> updatedOffsets = computeUpdatedOffsets(fireboltRecords);

        // Persist before updating local state so metadata failures do not advance in-memory offsets.
        if (fireboltMetadataService != null && !updatedOffsets.isEmpty()) {
            fireboltMetadataService.updateOffsets(topicName, updatedOffsets);
        }
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

    private Map<Integer, Long> computeUpdatedOffsets(List<AbstractFireboltRecord> fireboltRecords) {
        Map<Integer, Long> updatedOffsets = new HashMap<>();

        for (AbstractFireboltRecord fireboltRecord : fireboltRecords) {
            Integer partition = fireboltRecord.getPartition();
            Long offset = fireboltRecord.getOffset();
            Long lastProcessedOffset = processedPartitionOffsets.get(partition);

            if (lastProcessedOffset == null || offset > lastProcessedOffset) {
                updatedOffsets.merge(partition, offset, Math::max);
            }
        }
        return updatedOffsets;
    }

}
