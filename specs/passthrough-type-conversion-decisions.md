# Passthrough ingestion — remaining CI failures are engine assignment-cast gaps

The connector is **state-free** and does **no** data parsing: it ships each record as-is
(`INSERT INTO t (<record fields>) SELECT <record fields> FROM read_parquet|read_json('upload://batch')`)
and relies entirely on Firebolt's **assignment casts**. After fixing every connector-side
issue, the remaining red CI is, with very few exceptions, conversions Firebolt does not
perform on assignment. The connector cannot fix these without re-introducing the per-type
coercion we deleted; they are engine decisions.

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

### 1. `text → numeric / boolean / bytea / double / real / integer / bigint` (dominant)
The serializer tests pervasively use `*FromString` / `*AsString` columns
(`bigDecimalFromString`, `booleanFromString`, `doubleFromString`, `byteaAsString`, …): the
JSON/record field is a **string** and the column is the numeric/boolean/bytea type. Firebolt
rejects `text → <type>` on assignment (`text can't be assigned to column ... of the type ...`).
These are standard Postgres assignment casts. This even fails the `willNotStopProcessing…`
tests, because their *valid* records are also string-encoded — every record hits the gap, so
split-and-retry DLQs them all and nothing lands.
**Engine fix:** support `text → numeric/boolean/bytea/double/real/int` as assignment casts.

### 2. `bigint → timestamp / timestamptz` (+ `array(bigint) → array(timestamp)`)
`read_parquet` honors the Parquet **DATE** logical type (date tests pass) but **not** the
**timestamp/timestamptz** logical types — they come back as `bigint`, which won't assign to a
`TIMESTAMP` column. Also affects schemaless epoch-number timestamps.
**Engine fix:** `read_parquet` should surface Parquet timestamp logical types as TIMESTAMP
(and/or support `bigint → timestamp` epoch assignment).

### 3. JSON-Schema `Date` → "Date can only be used with an underlying int type"
The `kafka-connect-json-schema-converter` produces a Connect `Date` logical type whose base
isn't INT32, which `AvroData` rejects. Affects `DateSchemaSerializerTest` `LocalDate` fields.
**Options:** engine/converter alignment, or treat as unsupported (use ISO-string dates, which
work).

### 4. `struct → json` (single test: `AvroJsonSerializerTest.testAvroJsonAsNestedRecordSerialization`)
A nested record into a `JSON` column. `read_*` surfaces a STRUCT; Firebolt has no
`struct → json` assignment cast (Postgres uses `to_jsonb`). Nested → `STRUCT` columns work.

## To get CI green
Two honest paths, both yours to choose:
- **(recommended) Fix the engine** assignment casts in #1/#2 (and converter alignment for #3).
  The tests then pass for real and the connector ingests these types as users expect.
- **Disable the affected tests** (`@Disabled` with a tracking reason) to make CI green now.
  This is fake-green: it documents the gaps but the connector still can't ingest those types.

Gutting the tests to send only native types would hide that Firebolt should support these
(Postgres-standard) casts, so I have not done it unilaterally.
