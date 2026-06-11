package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.service.FireboltMetadataService;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.sink.SinkRecord;

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

    public void insertRecords(List<SinkRecord> records) throws SQLException {
        log.debug("Processing {} records for table: {}", records.size(), tableSchema.getTableName());

        if (CollectionUtils.isEmpty(records)) {
            return;
        }

        ingestionService.addRecords(records);

        // update the processed offsets in Kafka node
        updateProcessedOffsets(records);
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

    private void updateProcessedOffsets(List<SinkRecord> records) {
        // Compute the new high-water marks without touching processedPartitionOffsets yet.
        Map<Integer, Long> updatedOffsets = new HashMap<>(processedPartitionOffsets);
        records.forEach(record -> {
            Integer partition = record.originalKafkaPartition();
            long offset = record.originalKafkaOffset();
            if (updatedOffsets.getOrDefault(partition, -1L) < offset) {
                updatedOffsets.put(partition, offset);
            }
        });

        // Persist first. If the DB write fails, local state stays at the old values so
        // the next batch retries persisting the same offsets rather than diverging.
        if (fireboltMetadataService != null) {
            fireboltMetadataService.updateOffsets(topicName, updatedOffsets);
        }

        // Advance local state only after a successful persist.
        processedPartitionOffsets = updatedOffsets;
    }

}
