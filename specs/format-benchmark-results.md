# Decision record — schema-carrying ingestion format

**Decision: schema-carrying records are written as an Avro container file compressed with
Snappy and ingested via `read_avro`.** (Schemaless JSON continues to go through `read_json`.)
This supersedes the earlier Parquet + `read_parquet` prototype.

## Why (summary of the local benchmark)

A local benchmark (`BenchmarkFormatComparison`, since removed) compared Parquet+`read_parquet`
against Avro+`read_avro` on a 12-column event schema, both sharing the `AvroData` → record step.
Net throughput was a wash; the decision turned on the secondary factors.

| format | serialize ms/100k | payload MB | server read+insert ms | rows/s |
|---|---:|---:|---:|---:|
| parquet (uncompressed) | 145 | 9.7 | 108 | 395k |
| parquet (snappy) | 144 | 2.3 | 89 | 428k |
| avro (uncompressed) | 73 | 13.4 | 161 | 428k |
| **avro (snappy)** | **77** | **3.7** | **148** | **444k** |
| avro (deflate) | 202 | 2.7 | 151 | 283k |

Avro + Snappy was chosen because:
- **Throughput parity** with the best Parquet option.
- **~2× cheaper to serialize** on the (horizontally-scalable) Connect worker than Parquet.
- **Snappy over deflate**: deflate compresses slightly smaller but costs ~2.6× the serialize CPU
  for no net throughput gain; Snappy is the cheap-CPU / good-ratio sweet spot.
- **`read_avro` honors Avro logical types** (`timestamp-millis`, `date`) — the Parquet path
  returned bigint for timestamps.
- **Eliminates the entire Parquet/Hadoop dependency stack** (parquet-avro, parquet-hadoop-bundle,
  hadoop-client-runtime/api/common + their Jetty/nimbus CVE exclusions). The shaded plugin jar
  dropped to ~21 MB. `org.apache.avro` was already required by `AvroData`; `snappy-java` is a
  small, widely-used dependency.

Caveat the benchmark could not measure: Parquet's smaller compressed payload and faster
server-side read favor it under cross-network upload to a shared cloud engine. With realistic
Kafka batch sizes (hundreds–thousands of records, single-digit-KB compressed payloads) the
difference is not decisive, and the simplification + logical-type win dominate.

## Decimal precision (resolved)

Confluent `AvroData` maps a Connect `Decimal` to an Avro `decimal(precision, scale)`. **The
precision comes from the source schema** — `AvroData` reads `connect.decimal.precision` from the
Connect schema. It only falls back to a default of **64** when the source schema declares no
precision (e.g. a hand-built `Decimal.schema(scale)` with no precision parameter). The engine caps
Avro decimal precision at 38, so a source precision > 38 (or the 64 default) is rejected.

Implication for the earlier open question: **the connector already takes the source schema's
precision** — no precision-narrowing assignment cast in Firebolt is needed, and we agree that
would be an anti-pattern. Real registered Avro / JSON-Schema decimals carry a precision ≤ 38 and
work as-is. The only failure mode is a source that genuinely declares precision > 38 (which the
engine can't store anyway) or one that declares none (hits the 64 default). If the latter ever
bites real connectors, the fix is connector-side (constrain the emitted precision), not an engine
cast.

For reference: Postgres *does* coerce `numeric` into a `numeric(p,s)` column on assignment by
applying the column's type modifier — rounding the scale and erroring if the integer digits
overflow `p`. That's the column type-modifier behavior, not a narrowing cast between two arbitrary
numeric types, so it isn't a precedent for adding narrowing casts to Firebolt.
