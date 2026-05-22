package com.firebolt.kafka.connect.e2e;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Captures throughput metrics from a single benchmark run.
 * Serialized to {@code build/reports/benchmark/results.json}.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BenchmarkResult {

    @JsonProperty("commit_sha")
    private String commitSha;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("duration_seconds")
    private double durationSeconds;

    @JsonProperty("total_records_produced")
    private long totalRecordsProduced;

    @JsonProperty("produce_rate_records_per_sec")
    private long produceRateRecordsPerSec;

    @JsonProperty("produce_throughput_mb_per_sec")
    private double produceThroughputMbPerSec;

    @JsonProperty("ingest_duration_seconds")
    private double ingestDurationSeconds;

    @JsonProperty("ingest_rate_records_per_sec")
    private long ingestRateRecordsPerSec;

    @JsonProperty("record_size_bytes")
    private int recordSizeBytes;
}
