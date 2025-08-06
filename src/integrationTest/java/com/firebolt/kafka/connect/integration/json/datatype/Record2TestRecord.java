package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;

/**
 * Test record for topic2 with Integer id, Float value, and BigInteger attribute fields.
 * Maps to table2 in Firebolt.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Record2TestRecord {
    
    /**
     * Required integer ID field - must not be null in Firebolt table.
     * Maps to INTEGER NOT NULL column.
     */
    private Integer id;
    
    /**
     * Required float value field - must not be null in Firebolt table.
     * Maps to REAL NOT NULL column.
     */
    private Float value;
    
    /**
     * Required big integer attribute field - must not be null in Firebolt table.
     * Maps to BIGINT NOT NULL column.
     */
    private BigInteger bigIntAttribute;
} 