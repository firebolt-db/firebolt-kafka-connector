package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * Test record for comprehensive Bytea (binary data) serialization testing with JSON Schema and Kafka Connect.
 * 
 * This record tests:
 * - Required vs optional BYTEA fields
 * - BYTEA arrays with nullable and non-nullable elements
 * - Proper null handling for BYTEA types
 * - JSON Schema validation for binary data formats
 * - End-to-end serialization from Kafka to Firebolt BYTEA columns
 * - Various binary data patterns and sizes
 * - Base64 encoding/decoding for JSON transport
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ByteaTestRecord {
    
    /**
     * Record identifier for test verification.
     */
    private Integer recordId;
    
    /**
     * Required binary data field - must not be null.
     * Maps to Firebolt BYTEA NOT NULL.
     * Stored as Base64 encoded string in JSON.
     */
    private byte[] requiredBytea;
    
    /**
     * Optional binary data field - can be null or omitted.
     * Maps to Firebolt BYTEA NULL.
     * Stored as Base64 encoded string in JSON.
     */
    private byte[] optionalBytea;
    
    /**
     * Required array where individual binary elements can be null.
     * Maps to Firebolt ARRAY(BYTEA NULL) NOT NULL.
     */
    private List<byte[]> requiredListWithNullableElements;
    
    /**
     * Required array where individual binary elements cannot be null.
     * Maps to Firebolt ARRAY(BYTEA NOT NULL) NOT NULL.
     */
    private List<byte[]> requiredListWithNonNullElements;
    
    /**
     * Optional array - entire array can be null/omitted, and elements can be null.
     * Maps to Firebolt ARRAY(BYTEA NULL) NULL.
     */
    private List<byte[]> optionalList;
    
    /**
     * Optional array where individual binary elements cannot be null.
     * Maps to Firebolt ARRAY(BYTEA NOT NULL) NULL.
     */
    private List<byte[]> optionalListWithNonNullElements;
} 