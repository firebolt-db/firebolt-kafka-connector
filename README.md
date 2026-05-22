# Firebolt Kafka Sink Connector

A Kafka Connect Sink Connector that delivers data from Apache Kafka topics into [Firebolt](https://www.firebolt.io/) tables.

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

### Working with message formats

The connector can ingest schemaless JSON and schema-based messages that Kafka Connect
converters turn into Connect records. Schema-based formats supported by the connector
include JSON Schema, Avro, and Protobuf (`proto`).

Configure the Kafka Connect `value.converter` for the format on your topic:

| Message format | Value converter | Notes |
|----------------|-----------------|-------|
| Schemaless JSON | `org.apache.kafka.connect.json.JsonConverter` with `value.converter.schemas.enable=false` | Message fields are read from the JSON object directly. |
| JSON with schemas | `io.confluent.connect.json.JsonSchemaConverter` | Requires Schema Registry. |
| Avro | `io.confluent.connect.avro.AvroConverter` | Requires Schema Registry. |
| Protobuf / proto | `io.confluent.connect.protobuf.ProtobufConverter` | Requires Schema Registry. |

For schema-based formats, set the Schema Registry URL on the converter:

```properties
value.converter.schema.registry.url=http://schema-registry:8081
```

Protobuf works through Confluent's `ProtobufConverter`; no connector-specific proto
compilation is required. Define proto fields with names that match the Firebolt table
columns, for example:

```proto
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message OrderEvent {
  int32 id = 1;
  string amount = 2; // Use strings for high-precision NUMERIC values.
  google.protobuf.Timestamp created_at = 3;
  repeated string tags = 4;
  optional string comment = 5;
}
```

Proto3 scalar fields without `optional` have default values rather than nulls. Use
`optional` when field presence matters, and model required fields by making the
corresponding Firebolt column `NOT NULL` and ensuring producers always set the field.
Use `google.protobuf.Timestamp` for Firebolt `TIMESTAMP` and `TIMESTAMPTZ`; repeated
fields map to Firebolt arrays.

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
| `ingestion.type` | No | `sql` | Ingestion mode: `sql` (INSERT via SQL) or `binary` (Parquet upload) |
| `exactlyOnce` | No | `false` | Enable exactly-once delivery semantics |
| `errors.tolerance` | No | `none` | Error tolerance: `none` (fail on error) or `all` (skip and report to DLQ) |
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
