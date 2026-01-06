package com.firebolt.kafka.connect.load.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.firebolt.kafka.connect.load.messagegenerator.MessageGenerator;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Schemaless JSON publisher that produces messages as JSON strings.
 */
public class JsonSchemalessKafkaMessagePublisher extends KafkaMessagePublisher<String> {

    private static final boolean INCLUDE_NULLS_DEFAULT = true;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Producer<String, String> kafkaProducer;
    private final MessageGenerator<?> messageGenerator;

    public JsonSchemalessKafkaMessagePublisher(
            String bootstrapServers,
            String kafkaApiKey,
            String kafkaApiSecret,
            MessageGenerator<?> messageGenerator) {
        this(bootstrapServers, kafkaApiKey, kafkaApiSecret, INCLUDE_NULLS_DEFAULT, messageGenerator);
    }

    public JsonSchemalessKafkaMessagePublisher(
            String bootstrapServers,
            String kafkaApiKey,
            String kafkaApiSecret,
            boolean includeNulls,
            MessageGenerator<?> messageGenerator) {
        super(bootstrapServers, kafkaApiKey, kafkaApiSecret);
        this.kafkaProducer = initializeSchemalessJsonProducer(bootstrapServers, kafkaApiKey, kafkaApiSecret, includeNulls);
        this.messageGenerator = messageGenerator;
    }

    private Producer<String, String> initializeSchemalessJsonProducer(String bootstrapServers, String kafkaApiKey, String kafkaApiSecret, boolean includeNulls) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // configuration for large messages (aligned with integration tests)
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 48000000);
        props.put("buffer.memory", "48000000");

        // batching & compression for higher throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64_000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Confluent Cloud Kafka auth (SASL_SSL)
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + kafkaApiKey + "' password='" + kafkaApiSecret + "';");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", 45000);

        // JSON serialization behavior for Connect JSON Converter
        props.put("json.oneof.for.nullables", includeNulls);
        props.put("json.default.property.inclusion", includeNulls ? "ALWAYS" : "NON_NULL");
        props.put("json.write.dates.iso8601", true);
        props.put("json.indent.output", false);

        return new KafkaProducer<>(props);
    }

    @Override
    protected Producer<String, String> getProducer() {
        return kafkaProducer;
    }

    @Override
    @SneakyThrows
    protected ProducerRecord<String, String> nextMessage(String topicName, int messageSequenceId) {
        Object next = messageGenerator.nextMessage(messageSequenceId);
        String value = (next instanceof String) ? (String) next : OBJECT_MAPPER.writeValueAsString(next);
        String key = topicName + messageSequenceId;
        return new ProducerRecord<>(topicName, key, value);
    }
}


