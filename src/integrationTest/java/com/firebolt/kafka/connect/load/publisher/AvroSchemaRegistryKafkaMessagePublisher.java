package com.firebolt.kafka.connect.load.publisher;

import com.firebolt.kafka.connect.load.LoadTestRecord;
import com.firebolt.kafka.connect.load.messagegenerator.MessageGenerator;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Publishes LoadTestRecord messages to Kafka as Avro, using Schema Registry.
 * Converts LoadTestRecord to GenericData.Record for Avro serialization.
 */
public class AvroSchemaRegistryKafkaMessagePublisher extends KafkaMessagePublisher<Object> {

    private static final int DECIMAL_SCALE = 9;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final Producer<String, Object> kafkaProducer;
    private final MessageGenerator<LoadTestRecord> messageGenerator;
    private final Schema avroSchema;

    public AvroSchemaRegistryKafkaMessagePublisher(
            String bootstrapServers,
            String kafkaApiKey,
            String kafkaApiSecret,
            String schemaRegistryUrl,
            String schemaApiKey,
            String schemaApiSecret,
            String avroSchemaJson,
            MessageGenerator<LoadTestRecord> messageGenerator) {
        super(bootstrapServers, kafkaApiKey, kafkaApiSecret);
        this.avroSchema = new Schema.Parser().parse(avroSchemaJson);
        this.kafkaProducer = initializeAvroProducer(
                bootstrapServers, kafkaApiKey, kafkaApiSecret,
                schemaRegistryUrl, schemaApiKey, schemaApiSecret);
        this.messageGenerator = messageGenerator;
    }

    private Producer<String, Object> initializeAvroProducer(
            String bootstrapServers,
            String kafkaApiKey,
            String kafkaApiSecret,
            String schemaRegistryUrl,
            String schemaApiKey,
            String schemaApiSecret) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64_000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + kafkaApiKey + "' password='" + kafkaApiSecret + "';");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", 45000);

        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", schemaApiKey + ":" + schemaApiSecret);
        props.put("auto.register.schemas", "false");
        props.put("use.latest.version", "true");
        props.put("latest.compatibility.strict", "false");

        return new KafkaProducer<>(props);
    }

    @Override
    protected Producer<String, Object> getProducer() {
        return kafkaProducer;
    }

    @Override
    protected ProducerRecord<String, Object> nextMessage(String topicName, int messageSequenceId) {
        LoadTestRecord record = messageGenerator.nextMessage(messageSequenceId);
        GenericData.Record avroRecord = toAvroRecord(record);
        return new ProducerRecord<>(topicName, topicName + messageSequenceId, avroRecord);
    }

    private GenericData.Record toAvroRecord(LoadTestRecord record) {
        GenericData.Record avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("colInteger", record.getColInteger());
        avroRecord.put("colBigint", record.getColBigint());
        avroRecord.put("colNumeric", record.getColNumeric() != null ? decimalToBytes(record.getColNumeric()) : null);
        avroRecord.put("colReal", record.getColReal());
        avroRecord.put("colDoublePrecision", record.getColDoublePrecision());
        avroRecord.put("colBoolean", record.getColBoolean());
        avroRecord.put("colText", record.getColText());
        avroRecord.put("colTimestamp", record.getColTimestamp() != null
                ? record.getColTimestamp().format(TIMESTAMP_FORMATTER) : null);
        return avroRecord;
    }

    private static ByteBuffer decimalToBytes(BigDecimal value) {
        BigDecimal scaled = value.setScale(DECIMAL_SCALE, RoundingMode.HALF_UP);
        return ByteBuffer.wrap(scaled.unscaledValue().toByteArray());
    }
}
