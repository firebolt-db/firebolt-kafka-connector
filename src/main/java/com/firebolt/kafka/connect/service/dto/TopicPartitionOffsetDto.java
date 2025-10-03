package com.firebolt.kafka.connect.service.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TopicPartitionOffsetDto {
    private String topic;
    private int partition;
    private long offset;
}
