package com.firebolt.kafka.connect.integration.json.datatype;

import java.math.BigDecimal;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test record class for testing NUMERIC data type serialization.
 * Uses BigDecimal for precise decimal arithmetic and follows the same pattern as other test records.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NumericTestRecord {
    
    private Integer recordId;
    
    private BigDecimal requiredNumeric;
    
    private BigDecimal optionalNumeric;

    private Byte optionalByte;

    private Short optionalShort;

    private Integer optionalInt;

    private Long optionalLong;

    private Float optionalReal;

    private Double optionalDouble;

    private String bigDecimalFromString;
    
    private List<BigDecimal> requiredListWithNullableElements;

    private List<BigDecimal> requiredListWithNonNullElements;

    private List<BigDecimal> optionalList;

    private List<BigDecimal> optionalListWithNonNullElements;

} 