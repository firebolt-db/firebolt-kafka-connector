package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Test record for comprehensive STRUCT serialization testing.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StructTestRecord {
    
    private Integer recordId;
    
    private TestStruct requiredStruct;
    
    private TestStruct optionalStruct;
    
    private List<TestStruct> requiredStructArray;
    
    private List<TestStruct> optionalStructArray;
    
    private List<TestStruct> requiredStructArrayWithNullableElements;
    
    private List<TestStruct> optionalStructArrayWithNullableElements;
} 