package com.firebolt.kafka.connect.load.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

    /**
     * When true, first batch is published synchronously and publish() returns; remaining batches
     * are published asynchronously at 1 second intervals until message count is reached.
     */
    private final boolean continuousPublishing;

    /**
     * Number of messages per batch when continuous publishing is enabled.
     */
    private final int batchSize;

    public KafkaMessagePublisher(String bootstrapServers, String kafkaApiKey, String kafkaApiSecret) {
        this(bootstrapServers, kafkaApiKey, kafkaApiSecret, false, 0);
    }

    /**
     * Constructor that supports continuous publishing with configurable batch size.
     *
     * @param bootstrapServers      the Kafka bootstrap servers
     * @param kafkaApiKey           the API key for the Kafka broker
     * @param kafkaApiSecret        the API secret for the Kafka broker
     * @param continuousPublishing  when true, first batch is sent synchronously and publish()
     *                               returns; remaining batches are sent asynchronously every 1 second
     * @param batchSize             number of messages per batch when continuousPublishing is true
     */
    public KafkaMessagePublisher(String bootstrapServers, String kafkaApiKey, String kafkaApiSecret,
                                 boolean continuousPublishing, int batchSize) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaApiKey = kafkaApiKey;
        this.kafkaApiSecret = kafkaApiSecret;
        this.continuousPublishing = continuousPublishing;
        this.batchSize = batchSize;
    }

    private static final long CONTINUOUS_BATCH_INTERVAL_MS = 1000;

    /**
     * Publishes messages to Kafka topic
     * @param topicName - the name of the topic
     * @param messageCount - number of messages
     */
    public void publish(String topicName, int messageCount) {
        if (continuousPublishing && batchSize > 0) {
            publishContinuous(topicName, messageCount);
            return;
        }
        publishAll(topicName, messageCount);
    }

    /**
     * Publishes first batch synchronously and returns; remaining batches are published
     * asynchronously at 1 second intervals until messageCount is reached.
     */
    private void publishContinuous(String topicName, int messageCount) {
        Producer<String, T> producer = getProducer();
        int firstBatchCount = Math.min(batchSize, messageCount);
        CountDownLatch firstBatchLatch = new CountDownLatch(firstBatchCount);
        long start = System.currentTimeMillis();

        sendBatch(producer, topicName, 1, firstBatchCount, firstBatchLatch);
        producer.flush();
        try {
            boolean completed = firstBatchLatch.await(Math.max(30L, firstBatchCount / 100), TimeUnit.SECONDS);
            long tookMs = System.currentTimeMillis() - start;
            log.info("Published first batch of {} messages. Completed: {}. Elapsed: {} ms",
                    firstBatchCount, completed, tookMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for first batch", e);
        }

        if (messageCount <= batchSize) {
            return;
        }

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "kafka-continuous-publisher");
            t.setDaemon(true);
            return t;
        });
        executor.submit(() -> {
            try {
                int nextId = firstBatchCount + 1;
                while (nextId <= messageCount) {
                    Thread.sleep(CONTINUOUS_BATCH_INTERVAL_MS);
                    int batchCount = Math.min(batchSize, messageCount - nextId + 1);
                    CountDownLatch batchLatch = new CountDownLatch(batchCount);
                    sendBatch(producer, topicName, nextId, batchCount, batchLatch);
                    producer.flush();
                    batchLatch.await(Math.max(30L, batchCount / 100), TimeUnit.SECONDS);
                    log.info("Published batch: messages {} to {} ({} total so far)",
                            nextId, nextId + batchCount - 1, nextId + batchCount - 1);
                    nextId += batchCount;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Continuous publishing interrupted");
            }
        });
        executor.shutdown();
    }

    private void publishAll(String topicName, int messageCount) {
        CountDownLatch latch = new CountDownLatch(messageCount);
        long start = System.currentTimeMillis();

        Producer<String, T> producer = getProducer();
        sendBatch(producer, topicName, 1, messageCount, latch);

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

    private void sendBatch(Producer<String, T> producer, String topicName, int startId, int count,
                          CountDownLatch latch) {
        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                log.error("Produce failed: {}", exception.getMessage());
            }
            latch.countDown();
        };
        for (int i = 0; i < count; i++) {
            int messageId = startId + i;
            producer.send(nextMessage(topicName, messageId), callback);
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
