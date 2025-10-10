package com.firebolt.kafka.connect.integration;

public class SchemaBaseIntegrationTest extends BaseIntegrationTest {

    /**
     * Generates a unique connector name for test runs.
     * @param connectorType The type/name of the connector (e.g., "integer-serializer-test")
     * @return A unique connector name with a random suffix
     */
    protected void generateUniqueConnectorName(String connectorType) {
        super.generateUniqueConnectorName(connectorType + "-schema");
    }

    protected static String generateTableName(String name) {
        return name + "_schema";
    }

    protected static String generateTopicName(String name) {
        return name + "-schema";
    }

}
