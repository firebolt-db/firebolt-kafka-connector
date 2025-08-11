# Quickstart: Firebolt Kafka Sink on Confluent Cloud (Custom Connect)

This guide shows how to package, upload, configure, and run the Firebolt Kafka Sink Connector on Confluent Cloud using the Custom Connect feature.

## Prerequisites
- Confluent Cloud account with a Kafka cluster and Schema Registry (optional).
- Custom Connect enabled on your environment.
- Java 11 and Docker (locally only for optional validation).
- Firebolt account credentials (Client ID/Secret) and a target database + engine.

## 1) Build the connector package
From the repo root, build the Confluent‑compatible archive:

- Build: `./gradlew build`
- Create Confluent archive: `./gradlew buildConfluentHubArchive`
- Result: `build/confluent/firebolt-kafka-connect-<version>.zip`

This ZIP contains the JAR and manifest expected by Custom Connect.

## 2) Upload to Confluent Cloud (Custom Connect)
- In Confluent Cloud, go to Connectors > Custom connectors > Add custom connector.
- Upload the ZIP: `build/confluent/firebolt-kafka-connect-<version>.zip`.
- When prompted for the connector class, use: `com.firebolt.kafka.connect.FireboltSinkConnector`.
- Save. The connector becomes available to your environment.

## 3) Prepare topics
Create or pick the Kafka topics you will sink to Firebolt, for example:
- Topic: `orders`
- Target Firebolt table: `orders_raw`

Optional CLI example: `confluent kafka topic create orders`.

## 4) Create a connector instance
In Confluent Cloud > Connectors, choose your custom Firebolt connector and configure:

Required settings
- `topics`: `orders`
- `jdbc.connection.url`: `jdbc:firebolt:<database>?engine=<engine>&account=<account>`
- `firebolt.clientId` (Secret)
- `firebolt.clientSecret` (Secret)
- `topic.to.table.mapping`: `orders:orders_raw`

Or in JSON
```json
{
  "name": "firebolt-orders-sink",
  "connector.class": "com.firebolt.kafka.connect.FireboltSinkConnector",
  "tasks.max": "1",
  "topics": "orders",
  "jdbc.connection.url": "jdbc:firebolt:my_db?engine=my_engine&account=my_account",
  "firebolt.clientId": "<client_id>",
  "firebolt.clientSecret": "<client_secret>",
  "topic.to.table.mapping": "orders:orders_raw"
}
```
Notes
- Store credentials as Confluent Cloud Secrets and reference them as shown.
- Secure you credentials by encrypting them in Confluent Cloud, see [Confluent Cloud guide](https://docs.confluent.io/platform/current/security/compliance/secrets/overview.html).
- You may map multiple topics: `topicA:table_a,topicB:table_b`.

## 5) Verify the pipeline
- Produce a few messages to `orders` (JSON or your chosen format/converter).
- In Connect, check the task status is Running.
- In Firebolt, query the target table: `SELECT COUNT(*) FROM orders_raw;`

## Troubleshooting
- Connector fails at startup: verify `jdbc.connection.url` (account, database, engine) and that the engine is running.
- Table not found: create the Firebolt table or correct `topic.to.table.mapping`.
- No data appears: confirm messages are arriving on the topic and the connector task is Running (not Paused/Failed).

## Cleanup
- Pause or delete the connector instance in Confluent Cloud.
- Optionally remove test topics and test tables in Firebolt.

---
Tip: For local iteration, you can deploy the JAR into the provided Docker test stacks with `./gradlew deployToKafkaConnect` and bring them up via the `src/integrationTest/docker/...` compose files.
