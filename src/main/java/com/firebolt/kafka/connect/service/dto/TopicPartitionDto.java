package com.firebolt.kafka.connect.service.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TopicPartitionDto {
    private String topic;
    private int partition;
}
