package com.firebolt.kafka.connect.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Produces E2E test records as schemaless JSON strings to Kafka.
 */
@Slf4j
public class JsonMessageProducer implements MessageProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Producer<String, String> producer;
    private final AtomicLong failedSendCount = new AtomicLong();

    public JsonMessageProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_000);
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void produce(String topicName, List<E2ETestRecord> records) {
        for (E2ETestRecord record : records) {
            String json = toJson(record);
            producer.send(new ProducerRecord<>(topicName, String.valueOf(record.getId()), json),
                    (metadata, exception) -> {
                        if (exception != null) {
                            failedSendCount.incrementAndGet();
                            log.error("Failed to produce JSON record id={}: {}", record.getId(), exception.getMessage());
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

    private static String toJson(E2ETestRecord record) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("id", record.getId());
        node.put("name", record.getName());
        node.put("value", record.getValue());
        node.put("timestamp", record.getTimestamp().toString());
        return node.toString();
    }
}
