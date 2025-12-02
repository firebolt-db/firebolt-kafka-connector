package com.firebolt.kafka.connect.load;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRecordFactory {

    // this string will be stored on 10 bytes in Java as well as Firebolt
    private static final String SAMPLE_UNICODE_STRING = "ĀĦƁȒʤ";

    /**
     * The approximate size in bytes of the message that will be generated.
     * This will be controlled by increasing the size of the text field accordingly assuming that each char is on 2 bytes as we will use ASCII chars.
     */
    private int recordSizeInBytes;

    public TestRecordFactory(int recordSizeInBytes) {
        this.recordSizeInBytes = recordSizeInBytes;
    }


    /**
     * Will create a record of approximate size in bytes
     */
    public LoadTestRecord aValidRecord(int recordId) {
        return LoadTestRecord.builder()
                    .colInteger(recordId)
                    .colBigint(1000L)
                    .colNumeric(new BigDecimal("12345678901234567890123456789.123456789")) // Full NUMERIC(38,9) precision
                    .colReal(1.5f)
                    .colDoublePrecision(1.23456789)
                    .colBoolean(true)
                    .colText(aTextValue())
                    .colTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0))
                    .build();
    }

    private String aTextValue() {
        return SAMPLE_UNICODE_STRING.repeat(recordSizeInBytes/10);
    }

}
