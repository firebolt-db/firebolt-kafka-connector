package com.firebolt.kafka.connect.e2e;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Consistent test record schema used across all message types.
 * Fields: id (long), name (string), value (double), timestamp (timestamp).
 */
@Getter
@Builder
@AllArgsConstructor
public class E2ETestRecord {

    private final long id;
    private final String name;
    private final double value;
    private final Instant timestamp;

    /**
     * Creates a deterministic test record for the given sequence ID.
     * Name is padded to the requested size for throughput tests.
     */
    public static E2ETestRecord forSequenceId(long sequenceId, int namePadBytes) {
        String baseName = "record-" + sequenceId;
        String paddedName = padToSize(baseName, namePadBytes);
        return E2ETestRecord.builder()
                .id(sequenceId)
                .name(paddedName)
                .value(sequenceId * 1.1)
                .timestamp(Instant.parse("2024-01-01T00:00:00Z").plusSeconds(sequenceId))
                .build();
    }

    private static String padToSize(String base, int targetBytes) {
        if (targetBytes <= base.length()) {
            return base;
        }
        StringBuilder sb = new StringBuilder(targetBytes);
        sb.append(base);
        while (sb.length() < targetBytes) {
            sb.append('x');
        }
        return sb.toString();
    }
}
