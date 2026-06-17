# Firebolt Kafka Sink Connector

A Kafka Connect Sink Connector that delivers data from Apache Kafka topics into [Firebolt](https://www.firebolt.io/) tables.

## How it works

The connector is a **stateless passthrough**: it does not parse, buffer, or transform record
contents, and it never reads or caches your table's schema. It holds no local state — no cached
schemas, no in-memory buffers. (Kafka offsets are tracked in a Firebolt metadata table, not in the
connector.) Each poll batch is serialized as-is and handed to Firebolt, which parses and types it
**server-side**:

- **Schema-carrying records** — worker `value.converter` is the Avro, Protobuf, or JSON-Schema
  converter — are written to an Avro container (Snappy-compressed) and ingested with `read_avro`.
- **Schemaless JSON** — `org.apache.kafka.connect.json.JsonConverter` with `schemas.enable=false` —
  is written as NDJSON and ingested with `read_json`.

The connector builds each `INSERT` from the **record's own field names**, so a field lands in the
column of the same name. Because it never inspects the table definition, **Firebolt-side schema
evolution needs no connector change**: add a column and records that don't carry it get the
column's default, while records that do carry it populate it. (A record field with no matching
column fails that batch — the table is the contract.)

Type conversions are exactly Firebolt's **assignment casts** — the connector adds none.

### Delivery semantics

**At-least-once by default.** Set `exactlyOnce=true` to enable offset-based de-duplication: the
connector then records processed Kafka offsets in a Firebolt metadata table (persisted before
advancing local state) and skips records already ingested after a restart/rebalance. Either way, a
batch spanning multiple record schemas is written in a single transaction, so a partial failure
can't commit some rows while leaving their offsets behind.

### Error handling (dead-letter queue)

The connector uses Kafka Connect's standard error handling. With `errors.tolerance=all` and a
configured dead-letter queue, a record Firebolt rejects (or one that can't be converted) is routed
to the DLQ and processing continues — the batch is recursively split to isolate the offending
record so the good records still land. With `errors.tolerance=none` (the default) a rejected record
fails the task.

### Supported data types

The connector performs no coercion, so the supported set is exactly Firebolt's assignment-cast
matrix. Primary mappings (full matrix and edge cases in
[`specs/cast-semantics.md`](specs/cast-semantics.md)):

| Kafka Connect type | Firebolt column type |
|---|---|
| `INT8`, `INT16`, `INT32` | `INTEGER` (or any wider numeric) |
| `INT64` | `BIGINT` |
| `FLOAT32` | `REAL` |
| `FLOAT64` | `DOUBLE PRECISION` |
| `BOOLEAN` | `BOOLEAN` |
| `STRING` | `TEXT` (also → `INTEGER`/`BIGINT`/`REAL`/`DOUBLE`, and → `TIMESTAMP`/`TIMESTAMPTZ`/`DATE` from ISO-8601 strings) |
| `BYTES` | `BYTEA` (Avro / JSON-Schema paths — not schemaless JSON) |
| `Decimal` (logical) | `NUMERIC(p, s)` (precision defaults to 38 if the source declares none) |
| `Date` (logical) | `DATE` (Avro path; with JSON-Schema send an ISO-8601 date string) |
| `Timestamp` (logical, millis) | `TIMESTAMP` / `TIMESTAMPTZ` |
| `Array` | `ARRAY(...)` |
| `Struct` | `STRUCT(...)` or `JSON` |

**Not supported** (rejected by Firebolt on assignment, by design): `STRING`→`NUMERIC`/`BOOLEAN`/`BYTEA`,
and raw epoch numbers→`TIMESTAMP`/`DATE` (send ISO-8601 strings, or a typed `Timestamp`/`Date` logical).

## Requirements

- Apache Kafka Connect 3.2 or later
- Java 11 or later
- Firebolt account with a [service account](https://docs.firebolt.io/managing-your-organization/service-accounts/) (client ID and secret)

## Installation

Download the latest release artifacts from [GitHub Releases](https://github.com/firebolt-db/firebolt-kafka-connector/releases).

Each release includes:

| Artifact | Description |
|----------|-------------|
| `firebolt-kafka-connector-<version>.jar` | Uber JAR with all dependencies |
| `firebolt-db-firebolt-kafka-connect-<version>.zip` | Confluent Hub archive (for `confluent-hub` CLI) |
| `firebolt-kafka-connector-<version>-sources.jar` | Source code |

### Option A: Manual JAR install

1. Download `firebolt-kafka-connector-<version>.jar` from [Releases](https://github.com/firebolt-db/firebolt-kafka-connector/releases).
2. Copy the JAR into your Kafka Connect plugin directory:
   ```bash
   mkdir -p /path/to/kafka-connect/plugins/firebolt-kafka-connector
   cp firebolt-kafka-connector-*.jar /path/to/kafka-connect/plugins/firebolt-kafka-connector/
   ```
3. Ensure `plugin.path` in your Kafka Connect worker configuration includes the plugins directory:
   ```properties
   plugin.path=/path/to/kafka-connect/plugins
   ```
4. Restart Kafka Connect workers.

### Option B: Confluent Hub CLI

1. Download `firebolt-db-firebolt-kafka-connect-<version>.zip` from [Releases](https://github.com/firebolt-db/firebolt-kafka-connector/releases).
2. Install using the Confluent Hub CLI:
   ```bash
   confluent-hub install --no-prompt firebolt-db-firebolt-kafka-connect-<version>.zip
   ```
3. Restart Kafka Connect workers.

### Option C: Docker

Mount the uber JAR into your Kafka Connect container's plugin directory:

```yaml
services:
  kafka-connect:
    image: confluentinc/cp-kafka-connect:latest
    volumes:
      - ./firebolt-kafka-connector-<version>.jar:/usr/share/java/firebolt-kafka-connector/firebolt-kafka-connector.jar
    environment:
      CONNECT_PLUGIN_PATH: /usr/share/java
```

### Confluent Cloud

The connector is available as a verified plugin on [Confluent Hub](https://www.confluent.io/hub/firebolt-db/firebolt-kafka-connect). See the [Firebolt documentation](https://docs.firebolt.io/guides/integrations/kafka-sink-connector) for Confluent Cloud setup instructions.

## Configuration

Create a connector using the Kafka Connect REST API:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "firebolt-sink",
    "config": {
      "connector.class": "com.firebolt.kafka.connect.FireboltSinkConnector",
      "tasks.max": "1",
      "topics": "my-topic",
      "topic.to.table.mapping": "my-topic:my_table",
      "jdbc.connection.url": "jdbc:firebolt:my_database?engine=my_engine&account=my_account",
      "firebolt.clientId": "<your-service-account-client-id>",
      "firebolt.clientSecret": "<your-service-account-client-secret>",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }'
```

### Configuration Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `connector.class` | Yes | | Must be `com.firebolt.kafka.connect.FireboltSinkConnector` |
| `topics` | Yes | | Comma-separated list of Kafka topics to consume |
| `jdbc.connection.url` | Yes | | Firebolt JDBC URL (e.g., `jdbc:firebolt:my_db?engine=my_engine&account=my_account`) |
| `firebolt.clientId` | Yes | | Firebolt service account client ID |
| `firebolt.clientSecret` | Yes | | Firebolt service account client secret |
| `topic.to.table.mapping` | Yes | | Comma-separated mapping of topics to Firebolt tables (e.g., `topic1:table1,topic2:table2`) |
| `tasks.max` | No | `1` | Maximum number of tasks |
| `exactlyOnce` | No | `false` | When `true`, track ingested offsets in a Firebolt metadata table and skip re-delivered records (exactly-once); when `false`, at-least-once |
| `ingestion.type` | No | | **Deprecated and ignored.** Records are always ingested server-side via `read_avro` / `read_json` over `upload://`. Accepted for backwards compatibility. |
| `errors.tolerance` | No | `none` | Error tolerance: `none` (fail the task on error) or `all` (route bad records to the DLQ and continue) |
| `post.processing.script` | No | | Optional post-processing SQL to run after each batch (JSON format) |

## Building from Source

### Prerequisites
- Java 11 or higher

### Build
```bash
./gradlew build
```

### Run tests
```bash
./gradlew test
```

### Build Confluent Hub archive
```bash
./gradlew buildConfluentHubArchive
```

The archive will be at `build/confluent/firebolt-db-firebolt-kafka-connect-<version>.zip`.

## License

Apache License 2.0 -- see [LICENSE](LICENSE) for details.
