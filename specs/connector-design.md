# Firebolt Kafka Sink Connector — design overview

Reviewer's guide to the cleaned-up connector. Start here.

## One-line model

The connector is a **near-pure passthrough**: it ships each Kafka record to Firebolt as-is
over the `upload://` HTTP primitive and lets the **server** parse and type it. It performs no
data parsing and no type coercion of its own — the set of conversions it supports is exactly
Firebolt's **assignment-cast** matrix (see [cast-semantics.md](cast-semantics.md)).

```
INSERT INTO "<table>" BY NAME
SELECT * [, '<batch id>' AS "batch_id"]
FROM read_avro|read_json('upload://batch')
```

The connector **names no columns at all** — neither the table's nor the record's. `BY NAME` makes
Firebolt match each column the reader surfaces to the table column with the same name, so source
order doesn't matter. (A plain `INSERT INTO t SELECT *` would map by *position*, which is why earlier
revisions listed the record's fields on both sides; `BY NAME` removes that last piece of
column awareness.) Because the connector knows nothing about the table, schema evolution is free —
see "Record ↔ column matching" below. Type coercion is exactly Firebolt's
assignment casts; the connector adds none.

## Kafka Connect background (for reviewers new to it)

A Kafka Connect **sink connector** is plugin code that runs inside a Kafka Connect **worker** (a
JVM process). The worker — not our code — owns consuming from Kafka, committing offsets, and
deserializing record bytes. Two pieces matter here:

- **`value.converter`** (worker config): the class that turns a record's raw bytes into an
  in-memory Connect value *before the connector sees it*. With a schema-registry converter
  (`AvroConverter`, `JsonSchemaConverter`, `ProtobufConverter`) the record arrives as a typed
  `Struct` (a value + its Connect `Schema`); with the plain `JsonConverter` and
  `schemas.enable=false` it arrives as a schemaless `Map`. The connector's only job is to take that
  value and get it into Firebolt — it does not parse bytes itself. (Our `key.converter` is
  irrelevant; this is value-only.)
- **DLQ (dead-letter queue):** Kafka Connect's built-in error handling. When the worker is
  configured with `errors.tolerance=all` and `errors.deadletterqueue.topic.name=…`, records the
  connector reports as bad are routed to that Kafka topic instead of failing the task. With
  `errors.tolerance=none` (the default) a bad record fails the task instead. The connector receives
  an `ErrorReporter` from the framework and uses exactly this mechanism — it never invents its own.

Delivery is **at-least-once** by default; with `exactlyOnce=true` the connector tracks processed
offsets in a Firebolt metadata table (persisted before local advance) and skips already-ingested
records on restart. That offset table is the connector's only durable state, and only in the
exactly-once mode.

## Data flow

The Kafka Connect **worker's `value.converter`** deserializes the record bytes into a Connect
value *before* the connector sees it. The connector then routes by whether that value carries a
schema:

| `value.converter` | Connect value | Connector serializes to | Server TVF |
|---|---|---|---|
| `AvroConverter` | typed `Struct` | Avro container (snappy) via Confluent `AvroData` | `read_avro` |
| `JsonSchemaConverter` | typed `Struct` | Avro container (snappy) via `AvroData` | `read_avro` |
| `ProtobufConverter` | typed `Struct` | Avro container (snappy) via `AvroData` | `read_avro` |
| plain `JsonConverter`, `schemas.enable=false` | `Map` (no schema) | NDJSON | `read_json` |

So **schema-carrying records go through Avro + `read_avro`; schemaless JSON goes through
`read_json`.** A batch is grouped by value schema (schemaless JSON: by top-level key set, so a
key a record omits takes the column default — see "Remaining engine gaps"); each group is one
upload+INSERT, and multiple groups run in a single transaction (see Risks).

Class chain:
`FireboltSinkTask.put` → `AppendOnlyFireboltSinkService` → `TableWriter` (one per table) →
`IngestionService` (`UploadIngestionService`, optionally wrapped by
`IngestionServiceWithPostProcessing`) → `upload://` + `read_*`.

## What state actually remains in the connector

The connector no longer discovers or caches **any** table schema. `TableSchema` and all column
metadata fetching were removed — the connector only ever needs a table *name*, which it already
has from config. The ingestion path is **state-free**: `UploadIngestionService` holds only a JDBC
`Connection`, the **table name** (a `String`), the `ErrorReporter`, the error-tolerance flag, and
a stateless `AvroData` converter.

The only runtime state that remains:

| State | Where | Why |
|---|---|---|
| **Processed partition offsets** (only when `exactlyOnce=true`) | `TableWriter.processedPartitionOffsets`, persisted to a Firebolt metadata table | exactly-once replay protection; persisted before local advance. Default (`false`) is at-least-once and tracks nothing. |
| `topicToTableMapping`, `assignedTopicPartitions`, `errorToleranceAll` | `FireboltSinkTask` / `AppendOnlyFireboltSinkService` | routing + behavior config. |

**Table existence** is checked exactly once, at config-submission time, by
`FireboltSinkConnector.validate()` (`FireboltDbService.findNonExistentTables` → a config error if a
mapped table is missing). It is the single existence guard — nothing is cached, and there is no
per-task re-discovery. A table dropped while the connector runs surfaces as a normal batch failure
(task fails, or DLQ under error tolerance), consistent with "the table is the contract."

## Record ↔ column matching

`INSERT … BY NAME` matches **by name** (exact, case-sensitive) and order-independently. The three
cases (all verified against the engine):

| Case | Result |
|---|---|
| Record carries a **subset** of the table's columns | Works. Unnamed columns take their `DEFAULT` (or `NULL`). This is what makes **schema evolution** free: add a column to the table and old records — which simply don't name it — keep ingesting. |
| Record field name **matches** a column (any order) | Works. The field lands in the same-named column. Matching is exact and case-sensitive (`userid` ≠ `UserId`). |
| Record carries a field that is **not** a column | The batch **fails** with `Column '<x>' does not exist in the target INSERT table` — it is *not* silently discarded. |

The third case is intentional ("the table is the contract") and fails *loudly* — there's no data
corruption or silent drop. It is **not** a defect: the connector can't discard unknown fields
without either reading the table schema (which would re-introduce the state we removed) or a
Firebolt feature that ignores unmatched source columns. So the one schema-evolution scenario it
does *not* absorb is a producer adding a field **before** the column exists in Firebolt; that batch
fails until the column is added (or, with `errors.tolerance=all`, the offending records go to the
DLQ and the rest land). Tolerating that gracefully would need an engine-side name-based ingest
(an opt-in `BY NAME` mode that discards unmatched source columns) — a possible future ask, noted
here for the reviewer.

Two more `BY NAME` consequences:
- A record that carries a field named like a **literal column** (post-processing's `batch_id`) is
  rejected ("`BY NAME` matches the target column 'batch_id' more than once") — loudly, rather than
  letting the record's value silently break the post-processing script's batch filter.
- A batch consisting only of **empty JSON objects** `{}` is rejected by `read_json` (it can't infer a
  schema); mixed with non-empty records, `{}` lands as an all-defaults row.

## Remaining engine gaps

Verified against `ghcr.io/firebolt-db/engine:dev` (`5.0.0-pre.0.20260930181618.87bb548743ac`) over
`upload://`. None needs connector code; each is a server-side ask.

| Gap | Effect on the connector | Engine ask |
|---|---|---|
| `read_json` infers **one schema per upload** (a batch): a field whose JSON type differs across records (`1` vs `"x"`) fails the whole batch ("could not infer a consistent schema"), although each record alone would land | Batch fails; with DLQ on, split-and-retry lands every record individually (slow, but correct) | Type the reader by the `BY NAME` target (next row), or widen conflicting scalars to `text` |
| `read_json` **unions object shapes** across a batch: `{"k":1}` + `{"z":"q"}` into a `JSON` column stores `{"k":1,"z":null}` | **Silent fidelity loss** for nested objects into `JSON` columns (keys the producer never sent appear as `null`) | Under `INSERT … BY NAME`, type the reader by the target column (a `JSON` target reads the raw sub-document — what `SCHEMA => 'j json'` already does on files) |
| `read_json`: a field **absent** from one record but present in others surfaces as `NULL` for that record — overriding a nullable column's `DEFAULT` silently, and failing the whole batch for a `NOT NULL DEFAULT` column. Typical trigger: a batch straddling a producer schema change | **Mitigated in the connector**: schemaless records are uploaded one payload per top-level key set, so an absent key stays absent and the `DEFAULT` applies. Costs one `INSERT` per distinct shape in a batch; `json.consolidate.uploads=true` opts out (one upload, `NULL` instead of `DEFAULT`) for highly heterogeneous JSON — see the README | Target-typed reading with "absent" ≠ `null`; then the per-shape grouping and the option can be deleted |
| `read_json(..., SCHEMA => …)` rejects `upload://` ("not supported for COPY FROM") | The explicit-schema escape hatch isn't usable on the upload path | Support `SCHEMA` on `upload://` (ideally derived from the `BY NAME` target) |
| `read_json` nested object → `STRUCT` column must match the struct's fields **exactly** (a subset or extra keys fail) | Evolving nested objects fail; `read_avro` accepts a subset | Name-based, lenient struct assignment (missing → `NULL`) |
| `read_avro` rejects Avro **named-type references** ("Invalid Avro type : AVRO_NUM_TYPES") | Any Connect schema that reuses a named struct — e.g. an **unflattened Debezium envelope** (`before`/`after` share `Value`) — can't be ingested | Resolve named-type references in `read_avro` |
| Avro `map` surfaces as `array(struct(key, value))`, not assignable to `JSON` | Connect `MAP` fields only land in an `ARRAY(STRUCT(key, value))` column | `map` → `json` assignment |
| `text` → numeric / boolean / bytea and epoch `bigint` → timestamp / date are not assignment casts | Unchanged — see [cast-semantics.md](cast-semantics.md) | Product decision, not a bug |
| `read_avro` rejects Avro `decimal` *declaring* precision > 38, even when the values fit | A second *connector-side* engine workaround: the declared precision is capped at 38 on the writer schema (AvroData emits 64 for precision-less Decimals) | Accept precision > 38 and cast on assignment (fail only on overflow) |
| `BY NAME` has no "ignore unmatched source columns" mode | A producer adding a field before its column exists fails the batch | Opt-in discard of unmatched source columns |

## Cast semantics (summary)

The connector's supported conversions **are** Firebolt's assignment casts — we deliberately
mirror that logic rather than re-implement coercion. Full matrix + runnable probes:
[cast-semantics.md](cast-semantics.md). Headlines:

- **Works:** number→numeric/int/bigint/double/real (in range); `text`→int/bigint/double/real;
  `text`→timestamp/date/timestamptz (ISO-8601 strings); real binary→bytea; Avro
  `timestamp-millis`/`date` logical types; struct→json.
- **Not supported (rejected on assignment, by design):** `text`→numeric/boolean/bytea;
  raw epoch number→timestamp/date.

## Main risks / things to look at in review

1. **The table is the contract** (see "Record ↔ column matching"). Records carrying a *subset* of
   columns are fine (defaults fill the rest) — this is the schema-evolution path. A record field
   that is *not* a column fails the batch loudly (no silent drop / no corruption). The only
   not-absorbed case is a producer adding a field before its column exists. Covered by
   name-matching tests; a dedicated schema-evolution IT is added below.
2. **Offset/transaction correctness.** Multi-schema batches run as one transaction so a later
   group's failure can't leave earlier groups committed while offsets lag (which would duplicate
   on retry). With error-tolerance on, groups commit independently so split-and-retry can land the
   good records and DLQ the bad. Offsets are persisted *before* local advance.
3. **Split-and-retry DLQ isolation.** On an upload failure with error-tolerance on, the batch is
   halved recursively to isolate the offending record to the DLQ; without tolerance the failure
   propagates and the task fails. (Unit-tested.)
4. **Converter logical-type edges** — the subtle ones, all pushed to *supported representations*
   in the tests and documented in [cast-semantics.md](cast-semantics.md):
   - `JsonSchemaConverter` builds a Connect `Date` over a non-INT32 base, which `AvroData` rejects
     ("Date can only be used with an underlying int type") — so **JSON-Schema `Date` fields can't be
     ingested as a Date logical type; they must arrive as ISO-8601 date strings** (text→date). The
     Avro converter is unaffected (it produces a valid int32 Date). This is why the test fixtures
     have JSON-only ISO-date serializers — see that file's header and cast-semantics.md.
   - `read_json` rejects arrays of timestamp strings carrying a numeric offset (`+02:00`); only `Z`
     (UTC) works inside arrays. Scalars accept any offset.
   - `read_json` rejects subnormal doubles (underflow).
   - Decimals: `AvroData` requires a value's scale to equal the schema scale, and the connector caps
     the declared precision at 38 — see [cast-semantics.md](cast-semantics.md).
5. **Decimal/timestamp precision.** Connect `Timestamp` is millisecond precision (Avro
   `timestamp-micros` degrades), and Firebolt timestamps are microsecond precision — sub-unit
   values truncate.

## Test coverage

End-to-end coverage is heavy and is the main safety net for a server-parses-everything design.

**Unit tests** — `src/test`, ~220 tests, no Docker, run on every build:
- `UploadIngestionServiceTest` — the core: Avro & NDJSON round-trips, the `BY NAME` SQL, per-key-set
  JSON grouping, decimal precision cap, split-and-retry isolation, multi-group atomic transactions,
  tombstone skipping, literal columns, DLQ routing.
- Task/connector/services: `FireboltSinkTaskTest`, `FireboltSinkConnectorTest`,
  `AppendOnlyFireboltSinkServiceTest`, `FireboltDbServiceTest`, `FireboltMetadataServiceTest`,
  `TableWriterTest`.
- Config validators: `ConnectorConfigDefinitionTest`, `JdbcConnectionUrlValidatorTest`,
  `PostProcessingScriptValidatorTest`, `TopicToTableValidatorTest`, `SinkConfigTest`.
- Post-processing decorator: `IngestionServiceWithPostProcessingTest`.

**Integration tests** — `src/integrationTest`, full Docker stack (engine + Kafka + Connect +
Schema Registry), **every suite run on both KC 3.9.1 and KC 4.0**, sharded in CI by `@Tag`:
- **serialization (41 classes)** — the type matrix, three converter paths × ~14 data types:
  - Avro (`AvroConverter` → `read_avro`): BigInt, Boolean, Bytea, Date, Double, Integer, Json,
    Numeric, Real, Text, Timestamp, Timestamptz, AllDataTypes.
  - JSON-Schema (`JsonSchemaConverter` → `read_avro`): same set.
  - Schemaless JSON (`JsonConverter` → `read_json`): same set + `JsonColumnValue`,
    `JsonSchemalessIntegration`, `SchemalessWithTransforms`.
  - Each type test covers required/optional/null, arrays (nullable/non-null/empty/large/nested),
    edge values, and split-retry/DLQ poison handling, with and without serialized nulls.
- **connector (12):** `TableNameTest`, `ColumnNameTest` (schemaless), `ColumnNameJsonSchemaTest` (names that
  aren't Avro identifiers — dashes, dots, spaces, non-ASCII, nested STRUCT fields — through `read_avro`), `MultipleTopicsSerializerTest`,
  `DlqReporterIntegrationTest`, `ConnectorConfigurationTest`, `PostProcessingScript{Configuration,File}Test`,
  `SchemaEvolutionTest`, and `NameMatching{Schemaless,Avro}Test` — field ↔ column mismatches: extra
  table columns (nullable / `DEFAULT` / `NOT NULL DEFAULT` / `CURRENT_TIMESTAMP`), mixed shapes in one
  batch, explicit null vs absent, reordering, and extra / case-mismatched / missing-required fields
  (DLQ with tolerance, task failure without).
- **lifecycle (1):** `ConnectorManagementTest` (create/start/stop/restart/delete).
- **stress (1):** `LargePayloadTest`.
- **customer (1):** `Customer1IntegrationTest`.
- **e2e (1):** `E2EMessageTypeTest` — full pipeline across **JSON, AVRO, PROTOBUF**.
- **cloud (4 tagged):** excluded from core CI (run against managed cloud).

**Performance:** a **Throughput Benchmark** CI job runs on every PR; `LoadTest`/`ScenarioLoadTest`
are manual `./gradlew` harnesses. Coverage tooling: JaCoCo + SonarCloud.

**Deliberately disabled (2), each a documented limitation:**
| Test | Reason | Re-enable when |
|---|---|---|
| `ByteaSchemalessSerializerTest` | bytea via schemaless JSON = base64 `text`→bytea (unsupported) | never needed (bytea via Avro is covered), or engine adds `text`→bytea |
| `LargePayloadTest.willNotProcessSingleLargeMessage` | CI account 40 MB payload cap | run locally with a larger-limit account |

### Lines of code

| Area | LOC |
|---|---:|
| **Production** (`src/main`) | ~2,200 (25 classes) |
| **Unit tests** (`src/test`) | ~3,900 |
| **Integration tests** (`src/integrationTest`) | ~31,000 |

The integration matrix is intentionally the bulk: because all parsing/typing happens server-side,
behavior is only observable end-to-end, so the converter-path × data-type matrix is where
correctness is actually pinned. Keep that structure when adding types.

### Schema evolution
Schema evolution is a headline property of the state-free design. Two ITs evolve the table **and** the
producers mid-stream with no connector restart, and pause the connector while producing so that one
poll batch mixes pre- and post-change records (the case where defaults can go wrong):

- `integration/SchemaEvolutionTest` (schemaless JSON): `ADD COLUMN … NULL DEFAULT 7`,
  `ADD COLUMN … NOT NULL DEFAULT 'free'` and `DROP COLUMN` while running; old-shaped records get the
  defaults, new-shaped records their values, a producer that stops sending a field gets `NULL`, and a
  straggler still sending the dropped column goes to the DLQ.
- `integration/avro/SchemaEvolutionAvroTest` (Avro + Schema Registry): writer schemas v1 → v2 (adds an
  optional field, widens `int` → `long` into a `BIGINT` column) → v3 (drops a field) interleaved in one
  batch; a column absent from a record's writer schema takes its `DEFAULT`, while a field present in the
  schema with a `null` value is stored as `NULL`.

Firebolt has no `ALTER COLUMN … TYPE`, so type widening is covered on the producer side only. Field ↔
column mismatches outside evolution are in `NameMatching{Schemaless,Avro}Test` (above).
