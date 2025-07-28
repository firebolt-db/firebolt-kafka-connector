package com.firebolt.kafka.connect.service;

import java.util.Objects;
import lombok.Data;
import lombok.Getter;

@Data
@Getter
public final class TopicPartitionKey {

    private final String topic;
    private final Integer partition;

    /**
     * Creates a new TopicPartitionKey.
     *
     * @param topic the topic name (must not be null)
     * @param partition the partition number (can be null)
     */
    public TopicPartitionKey(String topic, Integer partition) {
        this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
        this.partition = partition;
    }

}