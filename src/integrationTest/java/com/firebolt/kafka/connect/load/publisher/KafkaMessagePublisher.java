package com.firebolt.kafka.connect.load.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Implementation of this class should know how to publish to Kafka topic.
 * T is the type of message that will be published.
 */
@Slf4j
public abstract class KafkaMessagePublisher<T> {

    /**
     * The Kafka bootstrap servers
     */
    private String bootstrapServers;

    /**
     * The api key and secret to talk to Kafka broker
     */
    private String kafkaApiKey;
    private String kafkaApiSecret;

    public KafkaMessagePublisher(String bootstrapServers, String kafkaApiKey, String kafkaApiSecret) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaApiKey = kafkaApiKey;
        this.kafkaApiSecret = kafkaApiSecret;
    }

    /**
     * Publishes messages to Kafka topic
     * @param topicName - the name of the topic
     * @param messageCount - number of messages
     */
    public void publish(String topicName, int messageCount) {
        CountDownLatch latch = new CountDownLatch(messageCount);
        long start = System.currentTimeMillis();

        Producer<String, T> producer = getProducer();
        for (int i = 1; i <= messageCount; i++) {

            ProducerRecord<String, T> producerRecord = nextMessage(topicName, i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        log.error("Produce failed: {}", exception.getMessage());
                    }
                    latch.countDown();
                }
            });
        }

        // Flush and wait bounded
        producer.flush();
        try {
            boolean completed = latch.await(Math.max(30L, messageCount / 100), TimeUnit.SECONDS);
            long tookMs = System.currentTimeMillis() - start;
            log.info("Published {} messages. Completed: {}. Elapsed: {} ms", messageCount, completed, tookMs);
        } catch (InterruptedException e) {
            log.error("There was an error flushing the records to kafka");
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the Kafka producer that will be used to send the messages to Kafka topic
     * @return
     */
    protected abstract Producer<String, T> getProducer();

    /**
     * Returns the next message to be published to the topic.
     * @param topicName - the name of the topic
     * @param messageCountId - the count of the message in the sequence of messages to be generated
     * @return
     */
    protected abstract ProducerRecord<String, T> nextMessage(String topicName, int messageCountId);

}
