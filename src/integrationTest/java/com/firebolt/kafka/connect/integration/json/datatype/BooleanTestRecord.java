package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Test record for comprehensive boolean serialization testing.
 * 
 * This record covers all BOOLEAN null/non-null scenarios:
 * - Required booleans (not null in DB)
 * - Optional booleans (nullable in DB) 
 * - Required lists with nullable elements
 * - Required lists with non-null elements
 * - Optional lists (nullable in DB)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BooleanTestRecord {
    
    /**
     * Required boolean field - must not be null in Firebolt table.
     * Maps to BOOLEAN NOT NULL column.
     */
    private Boolean requiredBoolean;
    
    /**
     * Optional boolean field - can be null in Firebolt table.
     * Maps to BOOLEAN NULL column.
     */
    private Boolean optionalBoolean;
    
    /**
     * Required list of booleans where the list itself cannot be null,
     * but individual elements within the list can be null.
     * Maps to ARRAY(BOOLEAN NULL) NOT NULL column.
     */
    private List<Boolean> requiredListWithNullableElements;
    
    /**
     * Required list of booleans where the list itself cannot be null,
     * and individual elements within the list cannot be null.
     * Maps to ARRAY(BOOLEAN NOT NULL) NOT NULL column.
     */
    private List<Boolean> requiredListWithNonNullElements;
    
    /**
     * Optional list of booleans - the entire list can be null.
     * Maps to ARRAY(BOOLEAN NULL) NULL column.
     */
    private List<Boolean> optionalList;

    /**
     * Optional list of booleans where individual elements cannot be null.
     * Maps to ARRAY(BOOLEAN NOT NULL) NULL column.
     */
    private List<Boolean> optionalListWithNonNullElements;

    /**
     * Record ID for test identification and ordering.
     */
    private Integer recordId;
}