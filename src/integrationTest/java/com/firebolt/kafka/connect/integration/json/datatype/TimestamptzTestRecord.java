package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Test record for comprehensive Timestamptz serialization testing with JSON Schema and Kafka Connect.
 * 
 * This record tests:
 * - Required vs optional Timestamptz fields (timezone-aware timestamps)
 * - Timestamptz arrays with nullable and non-nullable elements
 * - Proper null handling for Timestamptz types
 * - JSON Schema validation for timestamptz formats
 * - End-to-end serialization from Kafka to Firebolt TIMESTAMPTZ columns
 * - Kafka Connect Timestamp logical type (milliseconds since epoch) for timezone-aware storage
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TimestamptzTestRecord {
    
    /**
     * Record identifier for test verification.
     */
    private Integer recordId;
    
    /**
     * Required timestamptz field - must not be null.
     * Maps to Firebolt TIMESTAMPTZ NOT NULL.
     */
    private OffsetDateTime requiredTimestamptz;

    /**
     * Required array where individual timestamptz elements can be null.
     * Maps to Firebolt ARRAY(TIMESTAMPTZ NULL) NOT NULL.
     */
    private List<OffsetDateTime> requiredListWithNullableElements;

    /**
     * Required array where individual timestamptz elements cannot be null.
     * Maps to Firebolt ARRAY(TIMESTAMPTZ NOT NULL) NOT NULL.
     */
    private List<OffsetDateTime> requiredListWithNonNullElements;

    /**
     * Optional timestamptz field - can be null or omitted.
     * Maps to Firebolt TIMESTAMPTZ NULL.
     */
    private OffsetDateTime optionalTimestamptz;
    
    /**
     * Optional array - entire array can be null/omitted, and elements can be null.
     * Maps to Firebolt ARRAY(TIMESTAMPTZ NULL) NULL.
     */
    private List<OffsetDateTime> optionalList;
    
    /**
     * Optional array where individual timestamptz elements cannot be null.
     * Maps to Firebolt ARRAY(TIMESTAMPTZ NOT NULL) NULL.
     */
    private List<OffsetDateTime> optionalListWithNonNullElements;
    
    private String timestamptzString;

    /**
     * Array of timestamptz strings with microsecond precision in ISO-8601 format.
     * Allows preservation of microsecond precision through string representation.
     * Maps to Firebolt ARRAY(TIMESTAMPTZ NOT NULL) NOT NULL.
     */
    private List<String> timestamptzStringArray;
} 