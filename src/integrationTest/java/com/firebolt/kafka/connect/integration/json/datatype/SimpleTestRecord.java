package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

/**
 * Simple test record class that mirrors the structure of the simple test table.
 * This class provides a Java object representation with basic data types:
 * - id: BIGINT (Long)
 * - createdAt: TIMESTAMPTZ (Long) - epoch time in milliseconds for Kafka Connect Timestamp logical type
 * - recordTimestamp: BIGINT (Long) - epoch time in milliseconds for createdAt
 * - title: TEXT (String)
 * - description: TEXT (String)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimpleTestRecord {
    
    /**
     * Unique identifier - BIGINT NOT NULL
     */
    private Long id;
    
    /**
     * Creation timestamp with timezone - TIMESTAMPTZ
     * Stored as epoch milliseconds for Kafka Connect Timestamp logical type
     */
    private Long createdAt;
    
    /**
     * Epoch time in milliseconds for createdAt - BIGINT
     */
    private Long recordTimestamp;
    
    /**
     * Title text - TEXT
     */
    private String title;
    
    /**
     * Description text - TEXT
     */
    private String description;
} 