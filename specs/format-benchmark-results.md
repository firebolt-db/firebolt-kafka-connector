# Parquet vs Avro ingestion benchmark (local)

Question: for schema-carrying records, does the hop through **Parquet + `read_parquet`** beat
**Avro + `read_avro`**? Both share the `AvroData` → `GenericRecord` step (~20 ms / 10k records);
this measures only the format-dependent legs.

Harness: `BenchmarkFormatComparison` (integrationTest), run via `./gradlew runFormatBenchmark`
against a local engine (`release-5.0.1`). 12-column event schema (longs, strings, low-cardinality
country, doubles, int, bool, epoch, `array<string>`, a text comment). Median of 7, 2 warmup.
Numbers are local (loopback upload, single engine node) — see caveats.

## Throughput (100k records)

| format                | serialize ms | payload MB | server read+insert ms | rows/s (ser+insert) |
|-----------------------|-------------:|-----------:|----------------------:|--------------------:|
| parquet (uncompressed)|        145   |    9.7     |        108            |     395k            |
| parquet (snappy)      |        144   |    2.3     |         89            |     428k            |
| avro (uncompressed)   |         73   |   13.4     |        161            |     428k            |
| avro (snappy)         |         77   |    3.7     |        148            |     444k            |
| avro (deflate)        |        202   |    2.7     |        151            |     283k            |

(10k batch shows the same shape; at 1k Parquet's footer overhead makes it relatively worse.)

## Reading

- **Net throughput is a wash.** parquet-snappy (428k) ≈ avro-snappy (444k). Avro serializes
  ~2× cheaper on the client; Parquet reads ~1.7× faster on the server. They cancel.
- **Avro serialize is much cheaper** (73–77 ms vs 144 ms / 100k) — less CPU on the Connect worker.
- **Parquet payload is ~38% smaller** compressed (2.3 vs 3.7 MB) and reads faster server-side —
  matters more in production than locally (loopback hides upload bandwidth; a single local node
  understates shared-engine read cost).
- Avoid **deflate** (level 6): smallest but 2.6× the serialize cost of snappy for no net gain.

## The decisive non-perf finding: `read_avro` honors logical types

`read_parquet` returned **bigint** for Parquet timestamp logical types (only DATE worked) — the
gap behind the failing timestamp tests. `read_avro` does **not** have this gap. Verified through
the connector's actual path (`AvroData` → Avro → `read_avro`):

```
AvroData maps Connect Timestamp -> {long, logicalType: timestamp-millis}
AvroData maps Connect Date      -> {int,  logicalType: date}
read_avro ts -> timestamptz, assigns into TIMESTAMP  -> 2024-06-10 06:13:20  OK
read_avro d  -> date,        assigns into DATE        -> 2024-01-15           OK
```

So switching to Avro **fixes the timestamp/date gap with no engine change**.

## Open edge — Decimal precision

`AvroData` emits Connect `Decimal` as Avro `decimal` with **precision 64** (its default; Connect
Decimal carries only scale). The engine rejects precision > 38:
`Avro decimal with precision 64 is not supported (maximum supported precision is 38)`.
Independent of Parquet-vs-Avro; needs either the engine to cap/accept ≤38, or the connector to
constrain the emitted precision. Flagged, not yet handled.

## Recommendation

**Switch to Avro + `read_avro`**, drop the Parquet path:
- Net throughput parity.
- Fixes timestamp/date logical types (greens those tests without engine work).
- Eliminates the entire Parquet/Hadoop dependency stack (`parquet-avro`,
  `parquet-hadoop-bundle`, `hadoop-client-runtime`, `hadoop-common`) and its Jetty/nimbus CVE
  exclusions — the single biggest chunk of the connector's weight. `org.apache.avro` is already
  required by `AvroData`; `snappy-java` is already on the classpath.

Caveat the user should weigh: Parquet's smaller compressed payload and faster server-side read
favor it under **cross-network upload to a shared cloud engine** — conditions this local
benchmark cannot reproduce.
