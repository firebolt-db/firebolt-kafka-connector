# Passthrough ingestion — type-conversion decisions to work through

The connector ships records as-is and ingests them with a plain
`INSERT INTO t (cols) SELECT <field> ... FROM read_parquet|read_json('upload://batch')`
— **no connector-side casts**. So the supported conversions are exactly Firebolt's
**assignment casts**. This document is the corrected list after testing assignment
behavior directly on the engine (an earlier version was based on *explicit* `CAST`
errors and was wrong — e.g. it wrongly flagged double→real).

## Assignment behavior verified on the engine

✅ **Works on assignment (no connector handling needed):**
- `double → real`, `double → numeric(p,s)`, `int → integer/bigint`
- ISO-8601 string → `TIMESTAMP` / `TIMESTAMPTZ` / `DATE`
- JSON string containing JSON → `JSON` column (`text → json` is an assignment cast)
- identity / standard widenings

❌ **Genuine assignment gaps — these need a decision:**

### 1. Integer epoch → DATE / TIMESTAMP / TIMESTAMPTZ (and ARRAY variants)
```
1718000000000 (bigint)   → TIMESTAMPTZ   "bigint can't be assigned to ... timestamptz"
19737 (bigint, days)     → DATE          "bigint can't be assigned to ... date"
[1718000000000]          → ARRAY(TIMESTAMP)  "array(bigint) can't be assigned ..."
```
Hits sources that represent time as a **number** (JSON-Schema epoch fields; Connect
`Date`/`Timestamp` logical types serialize as epoch days/millis). ISO-8601 **strings**
already assign fine.
**Decision:** (a) engine: make `integer → temporal` an assignment cast (needs a unit
convention — s/ms/µs); or (b) data contract: require ISO strings / proper temporal
logical types and leave epoch-int unsupported. Open question for (b): does
`read_parquet` surface a Parquet `timestamp`/`date` **logical type** as `TIMESTAMP`/`DATE`
(assignable) rather than `bigint`? If yes, Avro logical-type sources work without change.

### 2. struct → JSON (and struct → TEXT)
```
{"k":"v"} (struct)  → JSON   "struct(...) can't be assigned to ... json"
```
A nested object that `read_json`/`read_parquet` surfaces as a `STRUCT`, targeting a
`JSON` (or `TEXT`) column. Consistent with Postgres (no composite→json cast; Postgres
uses `to_jsonb()`).
**Decision:** (a) engine: add a `struct → json` assignment cast (Firebolt already has
`TO_JSON`); or (b) data contract: model nested data as `STRUCT` columns (works today,
keeps types), reserve `JSON` columns for the whole-record case (`PARSE_AS_JSON`, already
handled), and accept JSON-as-a-string fields → `JSON` (also works today).

### 3. numeric(p,s) → numeric(p',s') widening
```
1.12::numeric(10,2)  → NUMERIC(38,2)   "numeric(10,2) can't be assigned to ... numeric(38,2)"
```
**Decision:** engine bug — lossless widening should be an assignment cast. Fix in the
engine; no connector change. (Confirmed via the standalone `INSERT INTO t(numeric(38,2))
SELECT 1.12::numeric(10,2)` repro.)

## Separate issue — per-record error isolation (not a conversion gap)
The `willNotStopProcessing…InvalidValues` tests send a few malformed values (`'abc'`→bigint,
`'invalid'`→boolean, non-ISO date) and expect the bad records DLQ'd and the rest ingested.
A single batched `INSERT … SELECT … read_xxx` means one un-castable value fails the **whole
batch**. **Decision:** accept batch-level DLQ; add split-and-retry to isolate the poison
record; or pre-validate connector-side.

## Local state / column matching (the "schema evolution" question)
Firebolt has no implicit name matching: `SELECT *` is positional (mismatched
`'alice'`→bigint), and `INSERT … BY NAME` is a syntax error. The connector can still avoid
querying the table by building the INSERT from the **record's own field names**:
`INSERT INTO t (userId, name) SELECT "userId","name" FROM read_json(...)` — verified, with
the unquoted insert-column folding to lowercase so camelCase keys match lowercase columns.
Trade-offs of going fully state-free:
- **Extra record fields error** (`Column 'junk' does not exist`) instead of being dropped — a data-contract change.
- **`PARSE_AS_JSON` auto-detection** (lone JSON column) needs to know the table shape, so the
  "store whole record as JSON" path would need an explicit config flag instead.
- Case handling relies on identifier folding; mixed-case quoted DDL columns wouldn't match.

In exchange: zero table state, and **Firebolt-side schema evolution works with no connector handling**.
