# Cast semantics — what the connector can ingest

The connector does **no** coercion of its own. Each record is shipped as-is and ingested with
`INSERT INTO t (<fields>) SELECT <fields> FROM read_avro|read_json('upload://batch')`, so the
operation that decides whether a value lands is a Firebolt **assignment cast** from the type
`read_avro`/`read_json` surfaces to the target column type.

**The connector's supported conversions therefore equal Firebolt's assignment-cast matrix — by
design.** This document is the reference for that matrix: it tells a future maintainer which
record shapes ingest cleanly and which are rejected, and gives runnable SQL probes to re-verify
against an engine. If you're tempted to "fix" a rejected conversion in the connector, don't —
either it's a supported assignment cast (then it already works) or it isn't (then it's an engine
decision, not the connector's job to paper over).

## What surfaces from each reader

| Source | `read_avro` / `read_json` surfaces it as |
|---|---|
| Avro `long`+`timestamp-millis` logical | `timestamptz` (assigns to TIMESTAMP/TIMESTAMPTZ) |
| Avro `int`+`date` logical | `date` |
| Avro `bytes` | `bytea` |
| Avro `decimal(p,s)` | `numeric(p,s)` (engine caps `p` ≤ 38) |
| JSON number | `double` (or `bigint` for integers) |
| JSON string | `text` |
| JSON object / Avro record | `struct(...)` |
| JSON array / Avro array | `array(...)` of the element type |

## Supported (these ingest cleanly)

- **Numbers → numeric / integer / bigint / double / real** (within range). Incl. `real→numeric`,
  `double→numeric`, `integer→numeric`.
- **`text` → integer / bigint / double / real** (numeric strings).
- **`text` → timestamp / timestamptz / date** — ISO-8601 strings. Timestamps **must include
  seconds** (`2024-01-01T12:00:15`, or space-separated, optional fractional, optional `Z`/offset).
- **Avro logical types**: `timestamp-millis` → TIMESTAMP/TIMESTAMPTZ, `date` → DATE.
- **Real binary → bytea** (Avro `bytes`; i.e. bytea works via the Avro/JSON-Schema paths).
- **`struct` → json**, and **object/array → STRUCT / ARRAY** columns (nested round-trips).
- **JSON string → json** column.

## Not supported (rejected on assignment — leave them to the engine)

- **`text` → numeric / boolean / bytea.** A string field targeting one of these column types is
  rejected. (So: bytea via *schemaless JSON* — which base64-encodes bytes to text — cannot work;
  use the Avro path, which carries real binary.)
- **Raw epoch number → timestamp / date.** `bigint → timestamp` is not an assignment cast. Epoch
  numbers from schemaless JSON don't land in TIMESTAMP/DATE columns — send ISO-8601 strings, or use
  a typed Avro/JSON-Schema `Timestamp` logical (millis).

## Boundary / precision behavior

- **`double → real`** works for all in-range values; rejected only at the float4 magnitude
  boundary (`Float.MAX_VALUE` ≈ 3.4e38) as "cannot be safely converted into type real".
- **Subnormal doubles** are rejected by `read_json` ("Cannot read floating point value:
  underflow").
- **Decimal scale** must match the source schema's declared scale (Confluent `AvroData`
  requirement). **Decimal precision** comes from the source schema; the engine caps it at 38.
  `AvroData` defaults to precision 64 only when the source declares none — see the decimal note in
  [format-benchmark-results.md](format-benchmark-results.md).
- **`read_json` arrays of timestamps**: elements must be `Z`/UTC (or offset-less); a numeric
  offset like `+02:00` inside an array is rejected (scalars accept any offset).
- **Connect `Timestamp` is millisecond precision**; Avro `timestamp-micros` degrades to a plain
  `bigint` through the converter→`AvroData` round-trip (→ unsupported). Firebolt timestamps are
  microsecond precision, so finer values truncate.

## Converter-specific note: JSON-Schema `Date`

`JsonSchemaConverter` builds a Connect `Date` logical type over a non-INT32 base (it defaults JSON
integers to int64 and ignores `connect.type: int32`), which Confluent `AvroData` rejects with
*"Date can only be used with an underlying int type."* The `AvroConverter` is **not** affected — it
produces a valid int32-based Connect `Date` that ingests fine. Practical rule: **with JSON-Schema,
send dates as ISO-8601 strings** (`text→date`), not as a Date logical type. (This is why the
integration fixtures carry JSON-only ISO-date serializers.)

## Runnable probes

Run top-to-bottom against any engine (e.g. a local `ghcr.io/firebolt-db/engine` container). The
"OK" statements succeed; the "REJECTED" ones error — that's the matrix above, in miniature.

```sql
CREATE TABLE probe (
    n NUMERIC(38,9), b BOOLEAN, by BYTEA,
    ts TIMESTAMP, tstz TIMESTAMPTZ, dt DATE, j JSON
);

-- Supported
INSERT INTO probe (n)    SELECT 42.42;                          -- OK  number  -> numeric
INSERT INTO probe (n)    SELECT 12.5::real;                     -- OK  real    -> numeric
INSERT INTO probe (n)    SELECT '42.42'::text::double precision;-- OK  text->double->numeric
INSERT INTO probe (ts)   SELECT '2024-06-10 06:13:20'::text;    -- OK  ISO string -> timestamp
INSERT INTO probe (tstz) SELECT '2024-06-10T06:13:20Z'::text;   -- OK  ISO+Z      -> timestamptz
INSERT INTO probe (dt)   SELECT '2024-01-15'::text;             -- OK  ISO date   -> date
INSERT INTO probe (j)    SELECT '{"k":"v"}'::text;              -- OK  JSON string-> json
INSERT INTO probe (b)    SELECT TRUE;                           -- OK  bool       -> boolean

-- Rejected (NOT assignment casts — by design)
INSERT INTO probe (n)    SELECT '42.42'::text;   -- REJECTED  text -> numeric
INSERT INTO probe (b)    SELECT 'true'::text;    -- REJECTED  text -> boolean
INSERT INTO probe (by)   SELECT 'DEADBEEF'::text;-- REJECTED  text -> bytea
INSERT INTO probe (ts)   SELECT 1718000000000::bigint; -- REJECTED  epoch bigint -> timestamp
INSERT INTO probe (dt)   SELECT 19737::bigint;         -- REJECTED  epoch days   -> date
```

Timestamp/bytea/struct logical-type behavior is best probed through `read_avro`/`read_json` over
`upload://` (the connector's actual path), since it depends on how the reader types the value —
see the integration serializer tests for end-to-end coverage.
