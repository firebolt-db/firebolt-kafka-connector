package com.firebolt.kafka.connect.e2e;

import java.io.Closeable;
import java.util.List;

/**
 * Produces test records to a Kafka topic in a specific serialization format.
 * Implementations handle JSON, Avro, and Protobuf serialization.
 */
public interface MessageProducer extends Closeable {

    /**
     * Produces the given records to the specified Kafka topic.
     *
     * @param topicName the Kafka topic to produce to
     * @param records   the test records to serialize and send
     */
    void produce(String topicName, List<E2ETestRecord> records);

    /**
     * Flushes any buffered records and waits for acknowledgments.
     */
    void flush();
}
