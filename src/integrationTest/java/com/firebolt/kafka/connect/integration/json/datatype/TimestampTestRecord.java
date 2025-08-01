package com.firebolt.kafka.connect.integration.json.datatype;

import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test record for comprehensive Timestamp serialization testing with JSON Schema and Kafka Connect.
 * 
 * This record tests:
 * - Required vs optional Timestamp fields
 * - Timestamp arrays with nullable and non-nullable elements
 * - Proper null handling for Timestamp types
 * - JSON Schema validation for timestamp formats
 * - End-to-end serialization from Kafka to Firebolt
 * - Kafka Connect Timestamp logical type (milliseconds since epoch)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TimestampTestRecord {
    
    /**
     * Record identifier for test verification.
     */
    private Integer recordId;
    
    /**
     * Required timestamp field - must not be null.
     * Maps to Firebolt TIMESTAMP NOT NULL.
     */
    private LocalDateTime requiredTimestamp;
    
    /**
     * Optional timestamp field - can be null or omitted.
     * Maps to Firebolt TIMESTAMP NULL.
     */
    private LocalDateTime optionalTimestamp;
    
    /**
     * Required array where individual timestamp elements can be null.
     * Maps to Firebolt ARRAY(TIMESTAMP NULL) NOT NULL.
     */
    private List<LocalDateTime> requiredListWithNullableElements;
    
    /**
     * Required array where individual timestamp elements cannot be null.
     * Maps to Firebolt ARRAY(TIMESTAMP NOT NULL) NOT NULL.
     */
    private List<LocalDateTime> requiredListWithNonNullElements;
    
    /**
     * Optional array - entire array can be null/omitted, and elements can be null.
     * Maps to Firebolt ARRAY(TIMESTAMP NULL) NULL.
     */
    private List<LocalDateTime> optionalList;
    
    /**
     * Optional array where individual timestamp elements cannot be null.
     * Maps to Firebolt ARRAY(TIMESTAMP NOT NULL) NULL.
     */
    private List<LocalDateTime> optionalListWithNonNullElements;
    
    /**
     * Microsecond precision timestamp as Long (microseconds since epoch).
     * This field preserves microsecond precision by bypassing Kafka Connect's Timestamp logical type.
     * Maps to Firebolt TIMESTAMP NOT NULL.
     */
    private Long microsecondTimestamp;

    /**
     * Array of microsecond precision timestamps as Longs (microseconds since epoch).
     * This field preserves microsecond precision by bypassing Kafka Connect's Timestamp logical type.
     * Maps to Firebolt ARRAY(TIMESTAMP NOT NULL) NOT NULL.
     */
    private List<Long> microsecondTimestampList;

    /**
     * Single timestamp string with microsecond precision.
     * Uses ISO-8601 format with microseconds (e.g., "2024-01-15T14:30:45.123456").
     * Maps to Firebolt TIMESTAMP NOT NULL.
     */
    private String timestampString;

    /**
     * Array of timestamp strings with microsecond precision.
     * Uses ISO-8601 format with microseconds (e.g., "2024-01-15T14:30:45.123456").
     * Maps to Firebolt ARRAY(TIMESTAMP NOT NULL) NOT NULL.
     */
    private List<String> timestampStringArray;

}