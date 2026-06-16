# Passthrough ingestion — type-conversion decisions to work through

The connector is **state-free**: it ships records as-is and ingests them with a plain
`INSERT INTO t (<record fields>) SELECT <record fields> FROM read_parquet|read_json('upload://batch')`
— no table-schema lookup, no casts. So the supported conversions are exactly Firebolt's
**assignment casts**, and the connector adds nothing.

This is the corrected list after running the full integration suite against assignment
behavior (an earlier version was based on *explicit* `CAST` errors and was wrong — e.g. it
mis-flagged double→real, which assignment handles fine).

## Status

**Fixed (connector-attributable) — green in CI:** the KC 4.0 dependency conflict
(excluded `kafka-schema-serializer` from the bundle), mixed-case column matching
(`ColumnNameTest`, `MultipleTopicsSerializerTest`), and the obsolete null-column-removal
optimization (deleted). All non-serialization suites pass: connector, lifecycle, stress,
e2e, customer, plus the throughput benchmark.

**Remaining (all serialization shards) — blocked on your decisions below.** Every failure
is a conversion Firebolt does not perform on assignment. They are *not* connector bugs and
won't be "fixed" in the connector without either engine changes or rewriting the tests to a
narrower contract — your call.

## Assignment-cast gaps (verified on the engine, by frequency)

| Conversion | Where it shows up |
|---|---|
| `bigint → timestamptz` / `timestamp` / `date` | epoch numbers (Connect Date/Timestamp logical types, JSON-Schema epoch fields) |
| `array(bigint) → array(timestamp)` / `array(date)` | array variants of the above |
| `text → boolean` | boolean sent as a JSON string (`"true"`) |
| `text → bytea` | bytea sent as a string (base64/hex) |
| `text → numeric(p,s)` | numeric sent as a string |
| `struct → json` | nested object → `JSON` column (Postgres has no such cast; uses `to_jsonb`) |
| `numeric(p,s) → numeric(p',s')` (widening) | a value of smaller precision into a wider column — almost certainly an engine bug |

ISO-8601 timestamp/date **strings**, `double→real`, `double→numeric`, `text→json`
(JSON-as-a-string), and all native types already assign cleanly — no action needed.

For each gap, the decision is the same shape: **(a) make it an assignment cast in the
engine** (the Postgres-consistent ones — text→boolean, text→bytea, numeric widening — look
like engine bugs/gaps), or **(b) declare it unsupported** and require the producer to send a
representation that assigns (ISO strings for time, native JSON booleans, `STRUCT` columns for
nested data).

## Separate issue — per-record error isolation
The `willNotStopProcessing…InvalidValues` tests send a few malformed values (`'abc'`→numeric,
non-ISO date) and expect the bad records DLQ'd and the rest ingested. A single batched
`INSERT … SELECT … read_xxx` fails the **whole batch** on one bad value. Decision: accept
batch-level DLQ; add split-and-retry to isolate the poison record; or pre-validate.

## Test alignment (after the above is decided)
A few tests also need updating to the state-free contract regardless: they mismatch JSON
field case vs column case (`ColumnNameTest` already aligned; `AllDataTypes*SerializerTest`
still references e.g. `colArrayDate` with non-matching case). These only become green once the
conversion gaps above are resolved, so they're deferred until then.
