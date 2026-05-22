package com.firebolt.kafka.connect.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Produces E2E test records as Avro GenericRecords via Schema Registry.
 */
@Slf4j
public class AvroMessageProducer implements MessageProducer {

    private static final String SCHEMA_RESOURCE = "/e2e/test-record.avsc";

    private final Producer<String, Object> producer;
    private final Schema avroSchema;
    private final AtomicLong failedSendCount = new AtomicLong();

    public AvroMessageProducer(String bootstrapServers, String schemaRegistryUrl) {
        this.avroSchema = loadSchema();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_000);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", "true");
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void produce(String topicName, List<E2ETestRecord> records) {
        for (E2ETestRecord record : records) {
            GenericRecord avroRecord = toAvroRecord(record);
            producer.send(new ProducerRecord<>(topicName, String.valueOf(record.getId()), avroRecord),
                    (metadata, exception) -> {
                        if (exception != null) {
                            failedSendCount.incrementAndGet();
                            log.error("Failed to produce Avro record id={}: {}",
                                    record.getId(), exception.getMessage());
                        }
                    });
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public long getFailedSendCount() {
        return failedSendCount.get();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private GenericRecord toAvroRecord(E2ETestRecord record) {
        GenericRecord avro = new GenericData.Record(avroSchema);
        avro.put("id", record.getId());
        avro.put("name", record.getName());
        avro.put("value", record.getValue());
        avro.put("timestamp", record.getTimestamp().toString());
        return avro;
    }

    private static Schema loadSchema() {
        try (InputStream is = AvroMessageProducer.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (is == null) {
                throw new IllegalStateException("Avro schema not found: " + SCHEMA_RESOURCE);
            }
            String json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(json);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Avro schema", e);
        }
    }
}
