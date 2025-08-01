package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstract base class for converting Kafka Connect SinkRecords to FireboltRecords.
 * Provides common functionality and defines the contract for specific converter implementations.
 */
@Slf4j
public abstract class RecordConverter {

    protected final SinkConfig config;

    /**
     * Constructor for RecordConverter implementations.
     *
     * @param config the sink configuration
     */
    protected RecordConverter(SinkConfig config) {
        this.config = config;
    }

    /**
     * Converts a Kafka SinkRecord to a FireboltRecord.
     * This is the main entry point for record conversion.
     *
     * @param record The Kafka SinkRecord to convert
     * @return The converted FireboltRecord
     * @throws RecordConversionException if conversion fails
     */
    public final FireboltRecord convert(SinkRecord record) throws RecordConversionException {
        // Delegate to specific implementation
        Map<String, KafkaMessageColumnValue> columnValues = convertRecordValue(record);

        String tableName = config.getTableNameForTopic(record.topic());

        if (tableName == null) {
            log.error("ERROR: No table mapping found for topic '{}'. Available mapping: '{}'",
                    record.topic(), config.getTopicToTableMapping());
            throw new RecordConversionException(
                    String.format("No table mapping found for topic: %s", record.topic()));
        }

        return new FireboltRecord(
                tableName,
                columnValues,
                record.topic(),
                record.kafkaPartition() != null ? record.kafkaPartition() : -1,
                record.kafkaOffset(),
                record.timestamp() != null ? record.timestamp() : System.currentTimeMillis()
        );
    }

    /**
     * Abstract method to be implemented by specific converters.
     * Converts the record value to a map of column values.
     *
     * @param record the SinkRecord to convert
     * @return map of column names to values
     * @throws RecordConversionException if conversion fails
     */
    protected abstract Map<String, KafkaMessageColumnValue> convertRecordValue(SinkRecord record) throws RecordConversionException;

    /**
     * Determines if this converter can handle the given record.
     *
     * @param record the SinkRecord to check
     * @return true if this converter can handle the record
     */
    public abstract boolean canHandle(SinkRecord record);

    /**
     * Returns a description of what types of records this converter handles.
     *
     * @return description string
     */
    public abstract String getDescription();


    /**
     * Handles null values in records.
     *
     * @param record the SinkRecord with null value
     * @return empty map for null values
     */
    protected final Map<String, KafkaMessageColumnValue> handleNullValue(SinkRecord record) {
        log.debug("Record value is null for topic={}, partition={}, offset={}",
                record.topic(), record.kafkaPartition(), record.kafkaOffset());
        return new HashMap<>();
    }

} 