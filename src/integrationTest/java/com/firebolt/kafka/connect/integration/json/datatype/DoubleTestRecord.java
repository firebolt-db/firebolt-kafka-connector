package com.firebolt.kafka.connect.integration.json.datatype;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test record class for testing DOUBLE PRECISION data type serialization.
 * Uses Double for floating-point arithmetic with 15 decimal-digit precision.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DoubleTestRecord {
    
    private Integer recordId;
    
    private Double requiredDouble;
    
    private Double optionalDouble;
    
    private Byte optionalByte;
    
    private Short optionalShort;
    
    private Integer optionalInt;
    
    private Long optionalLong;
    
    private Float optionalReal;
    
    private String doubleFromString;
    
    private List<Double> requiredListWithNullableElements;
    
    private List<Double> requiredListWithNonNullElements;
    
    private List<Double> optionalList;
    
    private List<Double> optionalListWithNonNullElements;

} 