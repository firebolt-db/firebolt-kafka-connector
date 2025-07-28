package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Test record for comprehensive REAL serialization testing.
 * 
 * This record covers all REAL null/non-null scenarios:
 * - Required real numbers (not null in DB)
 * - Optional real numbers (nullable in DB) 
 * - Required lists with nullable elements
 * - Required lists with non-null elements
 * - Optional lists (nullable in DB)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RealTestRecord {
    
    /**
     * Required real field - must not be null in Firebolt table.
     * Maps to REAL NOT NULL column.
     */
    private Float requiredReal;
    
    /**
     * Optional real field - can be null in Firebolt table.
     * Maps to REAL NULL column.
     */
    private Float optionalReal;
    
    /**
     * Required list of real numbers where the list itself cannot be null,
     * but individual elements within the list can be null.
     * Maps to ARRAY(REAL) NOT NULL column.
     */
    private List<Float> requiredListWithNullableElements;
    
    /**
     * Required list of real numbers where the list itself cannot be null,
     * and individual elements within the list cannot be null.
     * Maps to ARRAY(REAL) NOT NULL column.
     */
    private List<Float> requiredListWithNonNullElements;
    
    /**
     * Optional list of real numbers - the entire list can be null.
     * Maps to ARRAY(REAL) NULL column.
     */
    private List<Float> optionalList;

    private List<Float> optionalListWithNonNullElements;

    /**
     * Record ID for test identification and ordering.
     */
    private Integer recordId;
} 