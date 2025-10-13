package com.firebolt.kafka.connect;

/**
 * Base class for the value of a column from a Kafka message
 */
public interface KafkaMessageColumnValue {

    /**
     * Return the value of the column from the kafka message
     * @return
     */
    Object getValue();

}
