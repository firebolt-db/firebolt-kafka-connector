package com.firebolt.kafka.connect.load.messagegenerator;

/**
 * Class that knows how to generate a new message to be published to Kafka
 */
public interface MessageGenerator<T> {

    /**
     * Generates a new message.
     * @param messageSequenceId
     * @return
     */
    T nextMessage(int messageSequenceId);
}
