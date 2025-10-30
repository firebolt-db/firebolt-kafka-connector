package com.firebolt.kafka.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PostProcessingConfig {

    private List<Mapping> mappings;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Mapping {

        /**
         * The name of the table
         */
        private String table;

        /**
         * The script to run
         */
        private String script;

        /**
         * The path to a file containing the script to run
         */
        private String scriptFile;
    }

}
