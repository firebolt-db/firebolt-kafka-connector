package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test record for topic1 with Integer id and String text fields.
 * Maps to table1 in Firebolt.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Record1TestRecord {
    
    /**
     * Required integer ID field - must not be null in Firebolt table.
     * Maps to INTEGER NOT NULL column.
     */
    private Integer id;
    
    /**
     * Required text field - must not be null in Firebolt table.
     */
    private String text;
} 