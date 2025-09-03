package com.firebolt.kafka.connect.integration.json.datatype;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test record for comprehensive Text serialization testing with JSON Schema and Kafka Connect.
 * 
 * This record tests:
 * - Required vs optional Text fields
 * - Text arrays with nullable and non-nullable elements
 * - Proper null handling for Text types
 * - JSON Schema validation for text formats
 * - End-to-end serialization from Kafka to Firebolt TEXT columns
 * - Unicode character handling
 * - Large text data scenarios (up to 1MB)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TextTestRecord {
    
    /**
     * Record identifier for test verification.
     */
    private Integer recordId;
    
    /**
     * Required text field - must not be null.
     * Maps to Firebolt TEXT NOT NULL.
     */
    private String requiredText;
    
    /**
     * Optional text field - can be null or omitted.
     * Maps to Firebolt TEXT NULL.
     */
    private String optionalText;
    
    /**
     * Required array where individual text elements can be null.
     * Maps to Firebolt ARRAY(TEXT NULL) NOT NULL.
     */
    private List<String> requiredListWithNullableElements;
    
    /**
     * Required array where individual text elements cannot be null.
     * Maps to Firebolt ARRAY(TEXT NOT NULL) NOT NULL.
     */
    private List<String> requiredListWithNonNullElements;
    
    /**
     * Optional array - entire array can be null/omitted, and elements can be null.
     * Maps to Firebolt ARRAY(TEXT NULL) NULL.
     */
    private List<String> optionalList;
    
    /**
     * Optional array where individual text elements cannot be null.
     * Maps to Firebolt ARRAY(TEXT NOT NULL) NULL.
     */
    private List<String> optionalListWithNonNullElements;

    private Integer requiredInt;

    private Float requiredFloat;

    private Double requiredDouble;

    private Long requiredBigInt;

    private Boolean requiredBoolean;

    private BigDecimal requiredBigDecimal;

    private LocalDate requiredLocalDate;

    private LocalDateTime requiredLocalDateTime;

    private OffsetDateTime requiredTimestamptz;

} 