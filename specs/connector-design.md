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

The column list is built from **each record's own field names**, quoted on both sides, so a
field is matched to the column whose name equals it exactly (case-sensitive). Consequences, by
design:
- A record may carry a *subset* of the table's columns — absent columns take their default.
  **Firebolt-side schema evolution therefore needs zero connector handling.**
- A field that is **not** a column of the table fails the batch — the table is the contract.
- Type coercion is exactly Firebolt's assignment casts; the connector adds none.

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
| **Processed partition offsets** | `TableWriter.processedPartitionOffsets`, persisted to a Firebolt metadata table | drives at-least-once/idempotent offset tracking; persisted before local advance. |
| `topicToTableMapping`, `assignedTopicPartitions`, `errorToleranceAll` | `FireboltSinkTask` / `AppendOnlyFireboltSinkService` | routing + behavior config. |

**Table existence** is checked exactly once, at config-submission time, by
`FireboltSinkConnector.validate()` (`FireboltDbService.findNonExistentTables` → a config error if a
mapped table is missing). It is the single existence guard — nothing is cached, and there is no
per-task re-discovery. A table dropped while the connector runs surfaces as a normal batch failure
(task fails, or DLQ under error tolerance), consistent with "the table is the contract."

## Cast semantics (summary)

The connector's supported conversions **are** Firebolt's assignment casts — we deliberately
mirror that logic rather than re-implement coercion. Full matrix + runnable probes:
[cast-semantics.md](cast-semantics.md). Headlines:

- **Works:** numbers→numeric/int/bigint/double/real (in range), `text`→text/numeric? no —
  `text`→int/bigint/double/real and `text`→timestamp/date/timestamptz (ISO-8601 strings),
  real binary→bytea, Avro `timestamp-millis`/`date` logical types, struct→json (engine cast,
  landing).
- **Not supported (rejected on assignment, by design):** `text`→numeric/boolean/bytea,
  raw epoch number→timestamp/date.

## Main risks / things to look at in review

1. **The table is the contract.** Any record field without a matching column fails the *whole
   batch*. This is intended (and is what makes schema evolution free), but it means producer/table
   drift surfaces as a hard failure, not a silent skip. Covered by name-matching tests.
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

### Gap worth closing
Schema evolution is a headline property of the state-free design but isn't *directly* asserted —
a test that ingests, runs `ALTER TABLE … ADD COLUMN`, then ingests records carrying the new field
and verifies it lands would lock it in.
