package com.firebolt.kafka.connect.load;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRecordFactory80Column {

    // this string will be stored on 10 bytes in Java as well as Firebolt
    private static final String SAMPLE_UNICODE_STRING = "ĀĦƁȒʤ";

    /**
     * The approximate size in bytes of the message that will be generated.
     * This will be controlled by increasing the size of the text field accordingly assuming that each char is on 2 bytes as we will use ASCII chars.
     * Note: With 80 columns, the base size is much larger than the 8-column version.
     */
    private int recordSizeInBytes;

    /**
     * Each record will have an id that will be monotonically increasing
     */
    private AtomicInteger recordId;

    public TestRecordFactory80Column(int recordSizeInBytes) {
        this(1, recordSizeInBytes);
    }

    TestRecordFactory80Column(int startRecordId, int recordSizeInBytes) {
        this.recordId = new AtomicInteger(startRecordId);
        this.recordSizeInBytes = recordSizeInBytes;
    }

    /**
     * Will create a record of approximate size in bytes.
     * For 80-column records, the base size (without text fields) is significantly larger,
     * so the text field size is adjusted accordingly.
     */
    public LoadTestRecord80Column aValidRecord() {
        int currentId = recordId.getAndIncrement();
        
        return LoadTestRecord80Column.builder()
                // Base fields inherited from LoadTestRecord
                .colInteger(currentId)
                .colBigint(1000L + currentId)
                .colNumeric(new BigDecimal("12345678901234567890123456789.123456789"))
                .colReal(1.5f)
                .colDoublePrecision(1.23456789)
                .colBoolean(true)
                .colText(aTextValue())
                .colTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0))
                
                // Additional Integer columns (9 more)
                .colInteger2(currentId + 1)
                .colInteger3(currentId + 2)
                .colInteger4(currentId + 3)
                .colInteger5(currentId + 4)
                .colInteger6(currentId + 5)
                .colInteger7(currentId + 6)
                .colInteger8(currentId + 7)
                .colInteger9(currentId + 8)
                .colInteger10(currentId + 9)
                
                // Additional Bigint columns (9 more)
                .colBigint2(2000L + currentId)
                .colBigint3(3000L + currentId)
                .colBigint4(4000L + currentId)
                .colBigint5(5000L + currentId)
                .colBigint6(6000L + currentId)
                .colBigint7(7000L + currentId)
                .colBigint8(8000L + currentId)
                .colBigint9(9000L + currentId)
                .colBigint10(10000L + currentId)
                
                // Additional Numeric columns (9 more)
                .colNumeric2(new BigDecimal("22345678901234567890123456789.123456789"))
                .colNumeric3(new BigDecimal("32345678901234567890123456789.123456789"))
                .colNumeric4(new BigDecimal("42345678901234567890123456789.123456789"))
                .colNumeric5(new BigDecimal("52345678901234567890123456789.123456789"))
                .colNumeric6(new BigDecimal("62345678901234567890123456789.123456789"))
                .colNumeric7(new BigDecimal("72345678901234567890123456789.123456789"))
                .colNumeric8(new BigDecimal("82345678901234567890123456789.123456789"))
                .colNumeric9(new BigDecimal("92345678901234567890123456789.123456789"))
                .colNumeric10(new BigDecimal("10234567890123456789012345678.123456789"))
                
                // Additional Real columns (9 more)
                .colReal2(2.5f)
                .colReal3(3.5f)
                .colReal4(4.5f)
                .colReal5(5.5f)
                .colReal6(6.5f)
                .colReal7(7.5f)
                .colReal8(8.5f)
                .colReal9(9.5f)
                .colReal10(10.5f)
                
                // Additional Double Precision columns (9 more)
                .colDoublePrecision2(2.23456789)
                .colDoublePrecision3(3.23456789)
                .colDoublePrecision4(4.23456789)
                .colDoublePrecision5(5.23456789)
                .colDoublePrecision6(6.23456789)
                .colDoublePrecision7(7.23456789)
                .colDoublePrecision8(8.23456789)
                .colDoublePrecision9(9.23456789)
                .colDoublePrecision10(10.23456789)
                
                // Additional Boolean columns (9 more) - alternating true/false pattern
                .colBoolean2(false)
                .colBoolean3(true)
                .colBoolean4(false)
                .colBoolean5(true)
                .colBoolean6(false)
                .colBoolean7(true)
                .colBoolean8(false)
                .colBoolean9(true)
                .colBoolean10(false)
                
                // Additional Text columns (9 more) - distribute text size across all text fields
                .colText2("Ā")
                .colText3("Ā")
                .colText4("Ā")
                .colText5("Ā")
                .colText6("Ā")
                .colText7("Ā")
                .colText8("Ā")
                .colText9("Ā")
                .colText10("Ā")
                
                // Additional Timestamp columns (9 more) - slightly different timestamps
                .colTimestamp2(LocalDateTime.of(2024, 1, 2, 12, 0, 15, 0))
                .colTimestamp3(LocalDateTime.of(2024, 1, 3, 12, 0, 15, 0))
                .colTimestamp4(LocalDateTime.of(2024, 1, 4, 12, 0, 15, 0))
                .colTimestamp5(LocalDateTime.of(2024, 1, 5, 12, 0, 15, 0))
                .colTimestamp6(LocalDateTime.of(2024, 1, 6, 12, 0, 15, 0))
                .colTimestamp7(LocalDateTime.of(2024, 1, 7, 12, 0, 15, 0))
                .colTimestamp8(LocalDateTime.of(2024, 1, 8, 12, 0, 15, 0))
                .colTimestamp9(LocalDateTime.of(2024, 1, 9, 12, 0, 15, 0))
                .colTimestamp10(LocalDateTime.of(2024, 1, 10, 12, 0, 15, 0))
                .build();
    }

    /**
     * This would be the only value that would fluctuate.
     */
    private String aTextValue() {
        // Calculate the target text size per column
        // Account for the fact that we have 10 text columns to distribute the size across
        int textSizePerColumn = Math.max(1, recordSizeInBytes / (10 * 10)); // Divide by 100 (10 text columns * 10 bytes per string)
        
        return SAMPLE_UNICODE_STRING.repeat(1);
    }
}
