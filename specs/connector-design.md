# Firebolt Kafka Sink Connector — design overview

Reviewer's guide to the cleaned-up connector. Start here.

## One-line model

The connector is a **near-pure passthrough**: it ships each Kafka record to Firebolt as-is
over the `upload://` HTTP primitive and lets the **server** parse and type it. It performs no
data parsing and no type coercion of its own — the set of conversions it supports is exactly
Firebolt's **assignment-cast** matrix (see [cast-semantics.md](cast-semantics.md)).

```
INSERT INTO "<table>" (<record's own fields>)
SELECT <record's own fields>
FROM read_avro|read_json('upload://batch')
```

The connector **never reads the target table's schema.** The column list above is built purely
from **each record's own field names** — the names the worker's converter already attached to the
record — quoted on both sides. It lists them only because Firebolt has no name-based `INSERT`: a
bare `INSERT INTO t SELECT * FROM read_*(...)` maps the file's columns to the table's **by
position** (verified), which is fragile, and `INSERT ... BY NAME` is not supported. Naming the
record's own fields on both sides is how each field reliably lands in the column of the same name.
Because the connector only ever knows the *record's* fields and nothing about the table, schema
evolution is free — see "Record ↔ column matching" below. Type coercion is exactly Firebolt's
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
`read_json`.** A batch is grouped by value schema, so a mid-batch schema change yields one
upload+INSERT per schema, run in a single transaction (see Risks).

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

Because the connector names the record's own fields on both sides of the INSERT, matching is
**by name** and order-independent. The three cases (all verified against the engine):

| Case | Result |
|---|---|
| Record carries a **subset** of the table's columns | Works. Unnamed columns take their `DEFAULT` (or `NULL`). This is what makes **schema evolution** free: add a column to the table and old records — which simply don't name it — keep ingesting. |
| Record field name **matches** a column (any order) | Works. The field lands in the same-named column. |
| Record carries a field that is **not** a column | The batch **fails** with `Column '<x>' does not exist in the target INSERT table` — it is *not* silently discarded. |

The third case is intentional ("the table is the contract") and fails *loudly* — there's no data
corruption or silent drop. It is **not** a defect: the connector can't discard unknown fields
without either reading the table schema (which would re-introduce the state we removed) or a
Firebolt feature that ignores unmatched source columns. So the one schema-evolution scenario it
does *not* absorb is a producer adding a field **before** the column exists in Firebolt; that batch
fails until the column is added (or, with `errors.tolerance=all`, the offending records go to the
DLQ and the rest land). Tolerating that gracefully would need an engine-side name-based ingest
(e.g. `INSERT … BY NAME` with unmatched-source-column discard) — a possible future ask, noted here
for the reviewer.

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
   - Confluent `AvroData` requires a `Decimal` value's scale to equal the schema scale. For
     precision: the connector honors the *source* schema's precision and **defaults a precision-less
     Decimal to 38** (Firebolt's `NUMERIC(38, scale)` default) instead of AvroData's 64, which the
     engine (cap 38) would reject — done on the writer schema in `UploadIngestionService`. No
     precision-narrowing engine cast is required. See
     [format-benchmark-results.md](format-benchmark-results.md).
5. **Decimal/timestamp precision.** Connect `Timestamp` is millisecond precision (Avro
   `timestamp-micros` degrades), and Firebolt timestamps are microsecond precision — sub-unit
   values truncate.

## Test coverage

End-to-end coverage is heavy and is the main safety net for a server-parses-everything design.

**Unit tests** — `src/test`, ~225 tests, no Docker, run on every build:
- `UploadIngestionServiceTest` — the core: Avro & NDJSON round-trips, exact-name column matching,
  split-and-retry isolation, multi-group atomic transactions, tombstone skipping, literal columns,
  DLQ routing, empty-batch handling.
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
    edge values, and split-retry/DLQ poison handling, across an `includeNulls` × ingestion-mode
    parameter grid.
- **connector (7):** `TableNameTest`, `ColumnNameTest`, `MultipleTopicsSerializerTest`,
  `DlqReporterIntegrationTest`, `ConnectorConfigurationTest`, `PostProcessingScript{Configuration,File}Test`.
- **lifecycle (1):** `ConnectorManagementTest` (create/start/stop/restart/delete).
- **stress (1):** `LargePayloadTest`.
- **customer (1):** `Customer1IntegrationTest`.
- **e2e (1):** `E2EMessageTypeTest` — full pipeline across **JSON, AVRO, PROTOBUF**.
- **cloud (4 tagged):** excluded from core CI (run against managed cloud).

**Performance:** a **Throughput Benchmark** CI job runs on every PR; `LoadTest`/`ScenarioLoadTest`
are manual `./gradlew` harnesses. Coverage tooling: JaCoCo + SonarCloud.

**Deliberately disabled (3), each a documented limitation:**
| Test | Reason | Re-enable when |
|---|---|---|
| `ByteaSchemalessSerializerTest` | bytea via schemaless JSON = base64 `text`→bytea (unsupported) | never needed (bytea via Avro is covered), or engine adds `text`→bytea |
| `AvroJsonSerializerTest.testAvroJsonAsNestedRecordSerialization` | `struct`→json cast | the engine cast lands (in progress) |
| `LargePayloadTest.willNotProcessSingleLargeMessage` | CI account 40 MB payload cap | run locally with a larger-limit account |

### Lines of code

| Area | LOC | Notes |
|---|---:|---|
| **Production** (`src/main`) | **2,488** | 24 classes — the whole connector (down from 2,706 after removing schema discovery) |
| **Unit tests** (`src/test`) | **3,826** | ~219 tests |
| **Integration tests** (`src/integrationTest`) | **30,753** | full breakdown below |
| &nbsp;&nbsp;json/schema (read_avro) | 7,470 | |
| &nbsp;&nbsp;json/schemaless (read_json) | 6,870 | |
| &nbsp;&nbsp;avro (read_avro) | 4,346 | |
| &nbsp;&nbsp;integration/ (connector, lifecycle, stress, config, base classes) | 3,690 | |
| &nbsp;&nbsp;load (manual perf harnesses) | 2,675 | not run in CI |
| &nbsp;&nbsp;datatype fixtures (POJOs + serializers) | 1,587 | test data models |
| &nbsp;&nbsp;e2e | 1,376 | |
| &nbsp;&nbsp;customer | 179 | |

**~34.5k test LOC against 2.5k production LOC (~14:1).** The integration matrix is intentionally the
bulk: because all parsing/typing now happens server-side, behavior is only observable end-to-end,
so the converter-path × data-type matrix is where correctness is actually pinned. Keep that
structure when adding types.

### Schema evolution
Schema evolution is a headline property of the state-free design, so it has a dedicated IT:
`integration/SchemaEvolutionTest` ingests into a table, runs `ALTER TABLE … ADD COLUMN`, then
ingests records carrying the new field (and confirms older-shaped records still land with the new
column defaulted) — all with no connector restart, since the connector never caches the schema.
*Extensive* evolution coverage (drops, type widening, reordering across all converter paths) is a
sensible follow-up PR.
