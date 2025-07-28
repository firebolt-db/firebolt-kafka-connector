package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TestStruct {
    
    private String name;           // TEXT field
    
    private Integer age;           // INTEGER field
    
    private Boolean active;        // BOOLEAN field
    
    private Double score;          // DOUBLE PRECISION field
} 