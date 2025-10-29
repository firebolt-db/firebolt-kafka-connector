package com.firebolt.kafka.connect.load;

import java.util.Map;

public class BatchSizeConfig {

    private static final Map<Integer, Integer> BATCH_SIZES = Map.of(
        100, 50000,
        500, 20000,
        1000, 10000,
        5000, 6000,
        10000, 3000
    );

    public static int getBatchSize(int messageSizeBytes, String tableSchema) {
        return BATCH_SIZES.getOrDefault(messageSizeBytes, 3000);
    }
}
