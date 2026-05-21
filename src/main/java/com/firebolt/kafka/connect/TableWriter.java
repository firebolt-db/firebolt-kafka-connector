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

        Map<Integer, Long> offsetsToCommit = collectOffsetsToCommit(fireboltRecords);

        // Keep local offsets behind durable metadata if the metadata write fails.
        persistOffsetsIfExactlyOnceEnabled(offsetsToCommit);
        markOffsetsAsProcessed(offsetsToCommit);
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

    private Map<Integer, Long> collectOffsetsToCommit(List<AbstractFireboltRecord> fireboltRecords) {
        Map<Integer, Long> highestOffsetsByPartition = new HashMap<>();

        for (AbstractFireboltRecord fireboltRecord : fireboltRecords) {
            Integer partition = fireboltRecord.getPartition();
            Long recordOffset = fireboltRecord.getOffset();
            Long highestKnownOffset = highestOffsetsByPartition.get(partition);

            if (highestKnownOffset == null) {
                highestKnownOffset = processedPartitionOffsets.get(partition);
            }

            if (highestKnownOffset == null || recordOffset > highestKnownOffset) {
                highestOffsetsByPartition.put(partition, recordOffset);
            }
        }

        return highestOffsetsByPartition;
    }

    private void persistOffsetsIfExactlyOnceEnabled(Map<Integer, Long> offsetsToCommit) {
        if (fireboltMetadataService != null && !offsetsToCommit.isEmpty()) {
            fireboltMetadataService.updateOffsets(topicName, offsetsToCommit);
        }
    }

    private void markOffsetsAsProcessed(Map<Integer, Long> offsetsToCommit) {
        processedPartitionOffsets.putAll(offsetsToCommit);
    }

}
