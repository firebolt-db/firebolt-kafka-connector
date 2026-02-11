package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JsonRecord {
    private Integer recordId;
    private String requiredJson;

    @JsonIgnore
    private String expectedRequiredJson;
}
