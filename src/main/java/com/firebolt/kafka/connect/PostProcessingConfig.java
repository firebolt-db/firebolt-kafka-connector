package com.firebolt.kafka.connect;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PostProcessingConfig {

    private List<Mapping> mappings;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Mapping {

        /**
         * The name of the table
         */
        private String table;

        /**
         * The script to run
         */
        private String script;
    }

}
