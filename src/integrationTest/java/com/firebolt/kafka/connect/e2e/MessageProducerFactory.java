package com.firebolt.kafka.connect.e2e;

/**
 * Factory for creating the appropriate MessageProducer based on config.
 */
public final class MessageProducerFactory {

    private MessageProducerFactory() {}

    /**
     * Creates a MessageProducer for the given message type.
     *
     * @param messageType       the message serialization format
     * @param bootstrapServers  Kafka bootstrap servers
     * @param schemaRegistryUrl Schema Registry URL (required for Avro/Protobuf)
     * @return the appropriate MessageProducer implementation
     */
    public static MessageProducer create(
            E2EMessageType messageType,
            String bootstrapServers,
            String schemaRegistryUrl) {
        switch (messageType) {
            case JSON:
                return new JsonMessageProducer(bootstrapServers);
            case AVRO:
                return new AvroMessageProducer(bootstrapServers, schemaRegistryUrl);
            case PROTOBUF:
                return new ProtobufMessageProducer(bootstrapServers, schemaRegistryUrl);
            default:
                throw new IllegalArgumentException("Unsupported message type: " + messageType);
        }
    }
}
