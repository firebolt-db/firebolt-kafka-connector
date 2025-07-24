package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
public class TestStruct {
    
    private String name;           // TEXT field
    
    private Integer age;           // INTEGER field
    
    private Boolean active;        // BOOLEAN field
    
    private Double score;          // DOUBLE PRECISION field
} 