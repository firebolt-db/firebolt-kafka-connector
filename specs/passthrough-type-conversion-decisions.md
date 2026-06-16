# Passthrough ingestion — type-conversion decisions to work through

Context: the connector now uploads records as-is and ingests them server-side via
`INSERT INTO t (cols) SELECT CAST(<field> AS <target_type>) ... FROM read_parquet|read_json('upload://batch')`.
The full integration suite (PR #307, run 27590308320) surfaced cases where Firebolt's
`CAST` does not perform a conversion the old per-type converters used to do. None of
these are fixed in the connector — they need product/engine decisions.

**What already works** (so the boundary is clear): the `e2e` and `customer` suites pass;
direct validation passed for ints, text, booleans (real values), arrays of text, NUMERIC
with precision ≤ 38, ISO date/timestamp **strings**, Avro `float32` → `REAL`, nested object
→ `STRUCT` column, and whole-record → lone `JSON` column via `PARSE_AS_JSON`.

Counts below are summed across shards and inflated by connector batch-retries; treat them
as relative magnitude, not distinct cases.

---

## A. Real server-side conversion gaps (CAST can't bridge these)

### A1. Integer epoch → DATE / TIMESTAMP / TIMESTAMPTZ (and ARRAY variants) — largest
The connector uploads temporal values as the numbers the source provides (Connect
`Date`/`Timestamp` logical types serialize as epoch days/millis; JSON-Schema represents
time as plain numbers). `read_xxx` yields `bigint`/`array(bigint)`, and:

```sql
CAST("eventTime"  AS TIMESTAMPTZ)        -- value 1718000000000  → cannot cast bigint to timestamptz   (~383)
CAST("birthDate"  AS DATE)               -- value 19737 (epoch days) → cannot cast bigint to date        (~226)
CAST("eventTime"  AS TIMESTAMP)          -- → cannot cast bigint to timestamp                            (~144)
CAST("times"      AS ARRAY(TIMESTAMP))   -- value array(bigint) → cannot cast array(bigint) to array(timestamp) (~177)
CAST("dates"      AS ARRAY(DATE))        -- → cannot cast array(bigint) to array(date)                   (~48)
```
Affects both schema-carrying (Avro/JSON-Schema with epoch fields) and schemaless JSON
(epoch numbers). **Decisions:**
- Should `CAST(integer AS timestamp/date/...)` interpret the integer as an epoch? If so,
  what unit (seconds / millis / micros), and how is it chosen?
- Or should this stay unsupported and the contract require ISO-8601 **strings** (which already
  cast) / true temporal logical types? If logical types: does `read_parquet` honor a Parquet
  `timestamp`/`date` logical annotation (return TIMESTAMP/DATE, not bigint)? — needs an engine check.

### A2. Double → REAL (lossy narrowing refused)
`read_json` infers every JSON number as `double precision`; a `REAL` (float4) target is rejected:
```sql
CAST("temperature" AS REAL)   -- value 98.6 (double) → "Value of type double precision cannot be safely converted into type real"  (~32)
```
(Schema-carrying `float32` → `REAL` works; this is specifically schemaless JSON numbers → `REAL`.)
**Decision:** allow `CAST(double AS real)` as an explicit, possibly-lossy narrowing? Or require
the target column be `DOUBLE`?

### A3. struct → JSON (nested object into a JSON column among typed columns)
```sql
CAST("payload" AS JSON)   -- payload = {"name":...,"inner":{...}} → "cannot cast type struct(...) to type json"  (~32)
```
`read_json` infers a nested object as a `struct`; storing it in a `JSON` column fails.
(The lone-`JSON`-column case is already handled via `PARSE_AS_JSON`; this is a JSON column
*alongside* typed columns.) **Decision:** allow `CAST(struct AS json)`? Or is the lone-column
`PARSE_AS_JSON` path sufficient and nested-into-JSON-among-typed-columns out of scope?

---

## B. Architectural issue — per-record error isolation (not a conversion gap)

The `willNotStopProcessing…InvalidValues` tests deliberately send a few malformed values and
expect the connector to DLQ the bad records and ingest the rest:
```sql
CAST("bigIntAsString" AS BIGINT)   -- value 'abc'        → Unable to cast text 'abc' to bigint
CAST("realAsString"   AS REAL)     -- value '09-07-2025' → Could not convert text to real
CAST("flag"           AS BOOLEAN)  -- value 'invalid'    → Invalid input syntax for type BOOLEAN
-- also: text→integer/numeric(38,9)/double, text(non-ISO)→date
```
With a single batched `INSERT … SELECT … read_xxx`, **one un-castable value fails the whole
batch**, so valid records in the batch don't land either. The old connector used per-row
prepared statements with row-level isolation. **Decisions:**
- Accept coarser semantics (a cast failure DLQs / fails the whole batch)?
- Add split-and-retry (re-upload the batch in halves to isolate the poison record — the old
  binary path's recursive 413-splitting could be repurposed)?
- Pre-validate values connector-side before upload (reintroduces type-awareness)?

---

## Not affected (verified no errors in the suite)
BYTEA, NUMERIC/decimal with precision, plain integers/bigint, text, boolean (valid),
text arrays, ISO date/timestamp strings.
