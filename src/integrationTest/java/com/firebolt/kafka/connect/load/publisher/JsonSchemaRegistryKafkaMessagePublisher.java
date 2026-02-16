package com.firebolt.kafka.connect.load.publisher;

import com.firebolt.kafka.connect.load.messagegenerator.MessageGenerator;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class JsonSchemaRegistryKafkaMessagePublisher<T> extends KafkaMessagePublisher<T> {

    private static boolean INCLUDE_NULLS = true;

    private Producer<String, T> kafkaProducer;

    private MessageGenerator<T> messageGenerator;

    public JsonSchemaRegistryKafkaMessagePublisher(String bootstrapServers, String kafkaApiKey, String kafkaApiSecret,
                                                   String schemaRegistryUrl, String schemaApiKey, String schemaApiSecret, MessageGenerator<T> messageGenerator) {
        super(bootstrapServers, kafkaApiKey, kafkaApiSecret);
        this.kafkaProducer = initializeJsonProducer(INCLUDE_NULLS, bootstrapServers, kafkaApiKey,kafkaApiSecret, schemaRegistryUrl, schemaApiKey, schemaApiSecret);
        this.messageGenerator = messageGenerator;
    }

    public JsonSchemaRegistryKafkaMessagePublisher(String bootstrapServers, String kafkaApiKey, String kafkaApiSecret,
                                                   String schemaRegistryUrl, String schemaApiKey, String schemaApiSecret,
                                                   MessageGenerator<T> messageGenerator,
                                                   boolean continuousPublishing, int batchSize) {
        super(bootstrapServers, kafkaApiKey, kafkaApiSecret, continuousPublishing, batchSize);
        this.kafkaProducer = initializeJsonProducer(INCLUDE_NULLS, bootstrapServers, kafkaApiKey, kafkaApiSecret, schemaRegistryUrl, schemaApiKey, schemaApiSecret);
        this.messageGenerator = messageGenerator;
    }

    private <T> Producer<String, T> initializeJsonProducer(
            boolean includeNulls,
            String bootstrapServers,
            String kafkaApiKey,
            String kafkaApiSecret,
            String schemaRegistryUrl,
            String srApiKey,
            String srApiSecret) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        // batching & compression for higher throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);            // small delay to batch
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64_000);       // ~64KB per batch
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // or "snappy"/"zstd"
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Confluent Cloud Kafka auth (SASL_SSL)
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + kafkaApiKey + "' password='" + kafkaApiSecret + "';");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", 45000);

        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", srApiKey + ":" + srApiSecret);
        props.put("auto.register.schemas", "false");
        props.put("use.latest.version", "true");
        props.put("latest.compatibility.strict", "false");

        props.put("json.oneof.for.nullables", includeNulls);
        props.put("json.default.property.inclusion", includeNulls ? "ALWAYS" : "NON_NULL");
        props.put("json.write.dates.iso8601", true);
        props.put("json.indent.output", false);

        return new KafkaProducer<>(props);
    }

    @Override
    protected Producer<String, T> getProducer() {
        return kafkaProducer;
    }

    @Override
    protected ProducerRecord<String, T> nextMessage(String topicName, int messageSequenceId) {
        T value = messageGenerator.nextMessage(messageSequenceId);

        // will use the key of the message as topic name and sequenceId
        return new ProducerRecord<>(topicName, topicName + messageSequenceId, value);
    }
}
