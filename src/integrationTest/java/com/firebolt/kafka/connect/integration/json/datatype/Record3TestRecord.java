package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Test record for topic3 with Integer id and List<Integer> userIds fields.
 * Maps to topic3 table in Firebolt.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Record3TestRecord {
    
    /**
     * Required integer ID field - must not be null in Firebolt table.
     * Maps to INTEGER NOT NULL column.
     */
    private Integer id;
    
    /**
     * Required list of user IDs - must not be null in Firebolt table.
     * Maps to ARRAY(INTEGER) NOT NULL column.
     */
    private List<Integer> userIds;
} 