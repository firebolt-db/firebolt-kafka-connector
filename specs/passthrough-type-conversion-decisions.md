# Passthrough ingestion — remaining CI failures are engine assignment-cast gaps

The connector is **state-free** and does **no** data parsing: it ships each record as-is
(`INSERT INTO t (<record fields>) SELECT <record fields> FROM read_avro|read_json('upload://batch')`)
and relies entirely on Firebolt's **assignment casts**. After fixing every connector-side
issue, the remaining red CI is, with very few exceptions, conversions Firebolt does not
perform on assignment. The connector cannot fix these without re-introducing the per-type
coercion we deleted; they are engine decisions.

Schema-carrying records go through Avro + `read_avro` (not Parquet). `read_avro` honors Avro
logical types, so the Parquet-only timestamp gap below is resolved — see the local benchmark in
[format-benchmark-results.md](format-benchmark-results.md) for why Avro was chosen.

## Connector-side — DONE (green in CI)
- State-free, record-driven projection; exact-name column matching.
- KC 4.0 dependency conflict fixed by **shading** bundled `io.confluent.*` (was
  `NoSuchMethodError` on the runtime's Avro/JsonSchema converters).
- **Split-and-retry**: on upload failure with error tolerance on, the batch is split to
  isolate the offending record to the DLQ.
- Multi-group atomic transaction; Bugbot review items; obsolete optimization test removed.
- All non-serialization suites pass on KC 3.9.1 and 4.0 (connector, lifecycle, stress,
  e2e, customer) plus the throughput benchmark.

## Remaining failures — all need engine work (not connector)

### 1. `text → numeric / boolean / bytea` (dominant)
The serializer tests pervasively use `*FromString` / `*AsString` columns
(`bigDecimalFromString`, `booleanFromString`, `byteaAsString`, …): the JSON/record field is a
**string** and the column is the numeric/boolean/bytea type. Firebolt rejects `text → <type>`
on assignment (`text can't be assigned to column ... of the type ...`). These are standard
Postgres assignment casts. This even fails the `willNotStopProcessing…` tests, because their
*valid* records are also string-encoded — every record hits the gap, so split-and-retry DLQs
them all and nothing lands.
(Note: `text → double / real / integer / bigint` **already work** on assignment — only
numeric/boolean/bytea are gaps.)
**Engine fix:** support `text → numeric/boolean/bytea` as assignment casts.

### 2. `bigint → timestamp / timestamptz` — RESOLVED by the Avro switch (schema-carrying records)
`read_parquet` returned `bigint` for Parquet timestamp logical types. **`read_avro` does not** —
it surfaces Avro `timestamp-millis`/`timestamp-micros` as `timestamptz` and `date` as `date`, and
they assign cleanly into TIMESTAMP/DATE columns. Confirmed through the connector's actual path
(`AvroData` maps Connect `Timestamp` → `{long, timestamp-millis}`, Connect `Date` → `{int, date}`).
Remaining sub-case: **schemaless** epoch-number timestamps (a JSON number into a TIMESTAMP column)
still need `bigint → timestamp` on assignment, which the engine does not do — use ISO-8601 strings
for schemaless timestamps (those work via `text → timestamp`).

### 2b. Decimal precision > 38 (Avro)
`AvroData` emits Connect `Decimal` as Avro `decimal` with **precision 64** (its default — Connect
Decimal carries only scale). The engine caps Avro decimal precision at 38:
`Avro decimal with precision 64 is not supported (maximum supported precision is 38)`.
**Engine fix:** accept (cap to 38) Avro decimals with precision > 38. Alternatively the connector
could clamp the emitted precision, but that re-introduces per-type handling we are avoiding.

### 3. JSON-Schema `Date` → "Date can only be used with an underlying int type"
The `kafka-connect-json-schema-converter` produces a Connect `Date` logical type whose base
isn't INT32, which `AvroData` rejects. Affects `DateSchemaSerializerTest` `LocalDate` fields.
**Options:** engine/converter alignment, or treat as unsupported (use ISO-string dates, which
work).

### 4. `struct → json` (single test: `AvroJsonSerializerTest.testAvroJsonAsNestedRecordSerialization`)
A nested record into a `JSON` column. `read_*` surfaces a STRUCT; Firebolt has no
`struct → json` assignment cast (Postgres uses `to_jsonb`). Nested → `STRUCT` columns work.

### Not a gap — `RealSchemalessSerializerTest` (float4 overflow edge)
`double → real` assignment **works** for all in-range values; it is rejected only when the
magnitude overflows float4 (~3.4e38). The test fails purely on its `Float.MAX_VALUE` /
`-Float.MAX_VALUE` records, which overflow when a JSON number (double) is narrowed to REAL.
This is a test edge, not a missing cast — fix by dropping/adjusting those boundary records.

## To get CI green
Two honest paths, both yours to choose:
- **(recommended) Fix the engine** assignment casts in #1/#2 (and converter alignment for #3).
  The tests then pass for real and the connector ingests these types as users expect.
- **Disable the affected tests** (`@Disabled` with a tracking reason) to make CI green now.
  This is fake-green: it documents the gaps but the connector still can't ingest those types.

Gutting the tests to send only native types would hide that Firebolt should support these
(Postgres-standard) casts, so I have not done it unilaterally.
