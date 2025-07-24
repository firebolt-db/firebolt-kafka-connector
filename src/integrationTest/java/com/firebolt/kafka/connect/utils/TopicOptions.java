package com.firebolt.kafka.connect.utils;

/**
 * Configuration options for creating Kafka topics in integration tests.
 * Uses Lombok's builder pattern for flexible topic configuration.
 */
public class TopicOptions {
    
    private final int partitions;
    private final short replicationFactor;
    
    private TopicOptions(int partitions, short replicationFactor) {
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }
    
    public int getPartitions() {
        return partitions;
    }
    
    public short getReplicationFactor() {
        return replicationFactor;
    }
    
    public static TopicOptionsBuilder builder() {
        return new TopicOptionsBuilder();
    }
    
    public static class TopicOptionsBuilder {
        private int partitions = 1;
        private short replicationFactor = 1;
        
        public TopicOptionsBuilder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }
        
        public TopicOptionsBuilder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }
        
        public TopicOptions build() {
            return new TopicOptions(partitions, replicationFactor);
        }
    }
    
    /**
     * Creates TopicOptions with default values (1 partition, replication factor 1).
     * 
     * @return TopicOptions with default configuration
     */
    public static TopicOptions defaults() {
        return TopicOptions.builder().build();
    }
    
    /**
     * Creates TopicOptions with specified partitions and default replication factor (1).
     * Validates that partitions is positive.
     * 
     * @param partitions the number of partitions (must be positive)
     * @return TopicOptions with specified partitions and default replication factor
     * @throws IllegalArgumentException if partitions is not positive
     */
    public static TopicOptions withPartitions(int partitions) {
        validatePartitions(partitions);
        return TopicOptions.builder().partitions(partitions).build();
    }
    
    /**
     * Creates TopicOptions with specified partitions and replication factor.
     * Validates that both values are positive.
     * 
     * @param partitions the number of partitions (must be positive)
     * @param replicationFactor the replication factor (must be positive)
     * @return TopicOptions with specified configuration
     * @throws IllegalArgumentException if any parameter is not positive
     */
    public static TopicOptions of(int partitions, short replicationFactor) {
        validatePartitions(partitions);
        validateReplicationFactor(replicationFactor);
        return TopicOptions.builder()
                .partitions(partitions)
                .replicationFactor(replicationFactor)
                .build();
    }
    
    /**
     * Creates TopicOptions with specified partitions and replication factor (int overload).
     * Validates that both values are positive.
     * 
     * @param partitions the number of partitions (must be positive)
     * @param replicationFactor the replication factor (must be positive)
     * @return TopicOptions with specified configuration
     * @throws IllegalArgumentException if any parameter is not positive
     */
    public static TopicOptions of(int partitions, int replicationFactor) {
        return of(partitions, (short) replicationFactor);
    }
    
    /**
     * Validates that partitions is positive.
     * 
     * @param partitions the number of partitions to validate
     * @throws IllegalArgumentException if partitions is not positive
     */
    private static void validatePartitions(int partitions) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partitions must be positive, got: " + partitions);
        }
    }
    
    /**
     * Validates that replication factor is positive.
     * 
     * @param replicationFactor the replication factor to validate
     * @throws IllegalArgumentException if replicationFactor is not positive
     */
    private static void validateReplicationFactor(short replicationFactor) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be positive, got: " + replicationFactor);
        }
    }
} 