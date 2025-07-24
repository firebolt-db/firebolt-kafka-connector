package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Test record for comprehensive integer serialization testing.
 * 
 * This record covers all integer null/non-null scenarios:
 * - Required integers (not null in DB)
 * - Optional integers (nullable in DB) 
 * - Required lists with nullable elements
 * - Required lists with non-null elements
 * - Optional lists (nullable in DB)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IntegerTestRecord {
    
    /**
     * Required integer field - must not be null in Firebolt table.
     * Maps to INTEGER NOT NULL column.
     */
    private Integer requiredInteger;
    
    /**
     * Optional integer field - can be null in Firebolt table.
     * Maps to INTEGER NULL column.
     */
    private Integer optionalInteger;
    
    /**
     * Required list of integers where the list itself cannot be null,
     * but individual elements within the list can be null.
     * Maps to ARRAY(INTEGER) NOT NULL column.
     */
    private List<Integer> requiredListWithNullableElements;
    
    /**
     * Required list of integers where the list itself cannot be null,
     * and individual elements within the list cannot be null.
     * Maps to ARRAY(INTEGER) NOT NULL column.
     */
    private List<Integer> requiredListWithNonNullElements;
    
    /**
     * Optional list of integers - the entire list can be null.
     * Maps to ARRAY(INTEGER) NULL column.
     */
    private List<Integer> optionalList;

    private List<Integer> optionalListWithNonNullElements;

    /**
     * Record ID for test identification and ordering.
     */
    private Integer recordId;
} 