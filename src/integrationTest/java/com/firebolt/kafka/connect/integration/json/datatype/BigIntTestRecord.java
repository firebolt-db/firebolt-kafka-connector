package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Test record for comprehensive BIGINT serialization testing.
 * 
 * This record covers all BIGINT null/non-null scenarios:
 * - Required big integers (not null in DB)
 * - Optional big integers (nullable in DB) 
 * - Required lists with nullable elements
 * - Required lists with non-null elements
 * - Optional lists (nullable in DB)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BigIntTestRecord {
    
    /**
     * Required big integer field - must not be null in Firebolt table.
     * Maps to BIGINT NOT NULL column.
     */
    private Long requiredBigInt;
    
    /**
     * Optional big integer field - can be null in Firebolt table.
     * Maps to BIGINT NULL column.
     */
    private Long optionalBigInt;
    
    /**
     * Required list of big integers where the list itself cannot be null,
     * but individual elements within the list can be null.
     * Maps to ARRAY(BIGINT) NOT NULL column.
     */
    private List<Long> requiredListWithNullableElements;
    
    /**
     * Required list of big integers where the list itself cannot be null,
     * and individual elements within the list cannot be null.
     * Maps to ARRAY(BIGINT) NOT NULL column.
     */
    private List<Long> requiredListWithNonNullElements;
    
    /**
     * Optional list of big integers - the entire list can be null.
     * Maps to ARRAY(BIGINT) NULL column.
     */
    private List<Long> optionalList;

    private List<Long> optionalListWithNonNullElements;

    /**
     * Record ID for test identification and ordering.
     */
    private Integer recordId;

    /**
     * BigInt value represented as a string in JSON (e.g., "1234567890123").
     * Mapped to a BIGINT column in the database.
     */
    private String stringBigInt;

    /**
     * Optional short represented as a smaller integer type; schema uses connect.type int16.
     * Still mapped to BIGINT in Firebolt.
     */
    private Short optionalShort;

    /**
     * Optional int represented as connect.type int32; maps to BIGINT in Firebolt.
     */
    private Integer optionalInt;

    /**
     * Optional byte represented as connect.type int8; maps to BIGINT in Firebolt.
     */
    private Byte optionalByte;
} 