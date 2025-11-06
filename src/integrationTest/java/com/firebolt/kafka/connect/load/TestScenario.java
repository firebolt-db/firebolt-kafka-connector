package com.firebolt.kafka.connect.load;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class TestScenario {

    private String connectorName;

    private int nrOfKafkaMessageToProduce;

    private int averageMessageSizeInBytes;

    private String jsonSchemaRegistryDefinitionFilePath;

    private String tableSchemaDefinitionFilePath;

    /**
     * These are the static hostnames that will be used from the Kafka Connect cluster. Things like the id (id.staging.firebolt.io) or api (api.staging.firebolt.io) endpoints for firebolt
     */
    private Set<String> staticOutboundHostnames;

    /**
     * How much time to wait after the connector has been started to make sure all the records have been inserted in firebolt
     */
    private Duration fireboltIngestionWaitDuration;

    /**
     * Settings for the confluent cloud
     */
    private ConfluentCloudSettings confluentCloudSettings;

    /**
     * The name of the topic
     */
    private String topicName;

    private FireboltSettings fireboltSettings;

    /**
     * Some things we might want to change from one run to another (e.g how many records are being pulled from Kafka and at what interval)
     */
    private Map<String, String> connectorConfiguration;

    /**
     * Should we delete the connector at the end of a run
     */
    @Builder.Default
    private boolean deleteConnector = false;

    /**
     * Should we delete the table at the end of a run
     */
    @Builder.Default
    private boolean deleteTable = false;

    /**
     * Whether to use schemaless mode (no schema registry)
     */
    @Builder.Default
    private boolean schemaless = false;

    /**
     * Query to fetch records from Firebolt (instead of generating them)
     * If provided, records will be fetched from this query and published to Kafka
     */
    private String recordFetchQuery;

    /**
     * Post-processing script to run after insert
     * JSON format: { "mappings" : [ { "table" : "<table>", "script" : "<sql>" } ] }
     */
    private String postProcessingScript;

    @Override
    public String toString() {
        return String.format("TestScenario[connector='%s', messages=%d, messageSize=%d bytes, schemaless=%s, query=%s]", 
            connectorName, nrOfKafkaMessageToProduce, averageMessageSizeInBytes, schemaless, 
            recordFetchQuery != null ? "provided" : "none");
    }
}
