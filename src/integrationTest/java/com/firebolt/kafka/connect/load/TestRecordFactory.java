package com.firebolt.kafka.connect.load;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.apache.commons.lang3.RandomStringUtils;

public class TestRecordFactory {

    /**
     * The approximate size in bytes of the message that will be generated.
     * This will be controlled by increasing the size of the text field accordingly.
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
                    .colBigint(createRandomBigInt())
                    .colNumeric(randomBigDecimal())
                    .colReal(createRandomFloat())
                    .colDoublePrecision(createRandomDouble())
                    .colBoolean(true)
                    .colText(aTextValue())
                    .colTimestamp(createRandomLocalDateTime())
                    .build();
    }

    private String aTextValue() {
        return RandomStringUtils.secure().nextAlphabetic(recordSizeInBytes);
    }

    /**
     * Creates a random full decimal with 38 precision and 9 scale
     * @return
     */
    private BigDecimal randomBigDecimal() {
		StringBuilder digits = new StringBuilder(38);
		java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
		// Ensure first digit is non-zero to maintain precision 38
		digits.append((char) ('1' + rnd.nextInt(9)));
		for (int i = 1; i < 38; i++) {
			digits.append((char) ('0' + rnd.nextInt(10)));
		}
		// Insert decimal point to enforce scale 9 (38 total digits -> 29 integer, 9 fractional)
		digits.insert(38 - 9, '.');
		return new BigDecimal(digits.toString());
    }

    /**
     * Creates a random positive double value between 1.00000000 and 9.99999999
     * @return
     */
    private Double createRandomDouble() {
		java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
		int integerPart = 1 + rnd.nextInt(9); // 1..9 to ensure exactly one digit before decimal
		long fractionalPart = rnd.nextLong(100_000_000L); // 0..99_999_999
		return integerPart + (fractionalPart / 100_000_000.0d);
    }

    /**
     * Creates a positive big integer with a value in between 1000 and 9999
     * @return
     */
    private Long createRandomBigInt() {
		int value = 1000 + java.util.concurrent.ThreadLocalRandom.current().nextInt(9000); // 1000..9999
		return Long.valueOf(value);
    }

    /**
     * Creates a random float between 1.0 and 9.9
     * @return
     */
    private Float createRandomFloat() {
		java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
		int tenths = 10 + rnd.nextInt(90); // 10..99 -> 1.0..9.9
		return tenths / 10.0f;
    }

    /**
     * Create a random LocalDateTime. We can have the year as 2024, month as 1, day as 1. Just randomize the hour, minute and second.
     * @return
     */
    private LocalDateTime createRandomLocalDateTime() {
		java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
		int hour = rnd.nextInt(24);
		int minute = rnd.nextInt(60);
		int second = rnd.nextInt(60);
		return LocalDateTime.of(2024, 1, 1, hour, minute, second, 0);
    }

}
