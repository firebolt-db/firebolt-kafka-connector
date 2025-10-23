package com.firebolt.kafka.connect.load;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConfluentCloudSettings {

    /**
     * The cloud provider where the confluent environment is deployed to
     */
    @Builder.Default
    private String cloudName = "AWS";

    private String environmentId;
    private String clusterId;
    private String fireboltConnectorPluginId;

    // API key to interact with the Kafka broker
    private String kafkaApiKey;
    private String kafkaApiSecret;

    // API key for interacting with the schema registry
    private String schemaRegistryApiKey;
    private String schemaRegistryApiSecret;

    private String cloudResourceApiKey;
    private String cloudResourceApiSecret;

    private String exactlyOnce;
}
