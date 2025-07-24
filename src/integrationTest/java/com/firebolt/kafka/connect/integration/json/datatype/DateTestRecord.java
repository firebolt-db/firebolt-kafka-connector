package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDate;
import java.util.List;

/**
 * Test record for comprehensive Date serialization testing with JSON Schema and Kafka Connect.
 * 
 * This record tests:
 * - Required vs optional Date fields
 * - Date arrays with nullable and non-nullable elements
 * - Proper null handling for Date types
 * - JSON Schema validation for date formats
 * - End-to-end serialization from Kafka to Firebolt
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DateTestRecord {
    
    /**
     * Record identifier for test verification.
     */
    private Integer recordId;
    
    /**
     * Required date field - must not be null.
     * Maps to Firebolt DATE NOT NULL.
     */
    private LocalDate requiredDate;
    
    /**
     * Optional date field - can be null or omitted.
     * Maps to Firebolt DATE NULL.
     */
    private LocalDate optionalDate;
    
    /**
     * Required array where individual date elements can be null.
     * Maps to Firebolt ARRAY(DATE NULL) NOT NULL.
     */
    private List<LocalDate> requiredListWithNullableElements;
    
    /**
     * Required array where individual date elements cannot be null.
     * Maps to Firebolt ARRAY(DATE NOT NULL) NOT NULL.
     */
    private List<LocalDate> requiredListWithNonNullElements;
    
    /**
     * Optional array - entire array can be null/omitted, and elements can be null.
     * Maps to Firebolt ARRAY(DATE NULL) NULL.
     */
    private List<LocalDate> optionalList;
    
    /**
     * Optional array where individual date elements cannot be null.
     * Maps to Firebolt ARRAY(DATE NOT NULL) NULL.
     */
    private List<LocalDate> optionalListWithNonNullElements;
} 