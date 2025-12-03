## Demo: Local Kafka Connect (Firebolt Connector)

This folder contains helper scripts to spin up a local Kafka Connect environment (it will contain the Kafka Broker, Kafka Connect and the Schema registry) and interact with Kafka topics for quick demos and manual testing.

### Prerequisites
- Docker and Docker Compose installed and running
- Java/Gradle if you plan to build and deploy the connector locally

The stack uses the compose file at `src/integrationTest/docker/kafka-connect-cloud/docker-compose.yml`, and it will connect to the Firebolt Cloud so you need an account and clientId/clientSecret:

### Quick start
1. Build and deploy the connector jar:

```bash
./gradlew deployToKafkaConnect
```

The script above compiles the current code, creates a Kafka Sink Connector for firebolt and puts it into the plugins folder (that's how the Kakfa Connect runtime will load our Kafka Sink Connector when it sees the connector.class as com.firebolt.kafka.connect.FireboltSinkConnector)

2. Start the Kafka Connect stack:

```bash
# Starts the environment; add --deploy_latest to build+deploy first
./start_local_kafka_connect.sh
# or
./start_local_kafka_connect.sh --deploy_latest
```
NOTE: You can either do ./gradlew deployToKafkaConnect (which builds and deploys the connector) + ./start_local_kafka_connect.sh (which starts the local container) or just do it in one command: ./start_local_kafka_connect.sh --deploy_latest (which builds and deploys the code and then starts the local container)  

3. Create a topic:
   As a pre-requisite, the kafka topics and the Firebolt tables have to be created before the connector definition is uploaded to create a connector.

```bash
# Inside this folder
./kafka/create_topic.sh --topic demo-topic --partitions 1 --replication-factor 1
```
4. Edit the sample_connector_definition.json:
   You need to set the following attributes:
    - topics - set which topics you want to have monitored by the Firebolt Sink Connector
    - topics.to.table.mappings - in case the topic name does not match the table name in Firebolt, you will provide the mappings here
    - jdbc.connection.url - replace <database> with your own database name. Do the same with <account> and <engine> and set it to your account name and engine respectively
    - firebolt.clientId - put here the client id for the account that you will use to connect to Firebolt
    - firebolt.clientSecret - put here the client secret for the account that you will use to connect to Firebolt

NOTE: you also need to have the table names created in Firebolt as the connector will not autocreate them.

5. Create a Firebolt Sink Connector by uploading the definition:
```bash
# Create a new connector
./create_connector_definition.sh -f sample_connector_definition.json
```

6. The Firebolt tables have to be created ahead of time. You can find the schema and sql to create the students and champions_load_games tables under /demo/tableschemas

7. Generate messages to a topic:

```bash
# CSV samples are provided
./kafka/generate_messages.sh --topic demo-topic --file ./kafka/samples/students_sample.csv
```

Check the messages should be processed and appear in the table corresponding to the topic.

8. If you want to perform any sort of deduplication or post-processing script you need to setup this attribute in the connector definition:

- post.processing.script: " { "mappings" : [ "table":"table1", "script" : "<your script .... '${firebolt_params.batch_id}' "] }"

9. Stop the environment:

```bash
./stop_local_kafka_connect.sh
```

### Script reference
- `start_local_kafka_connect.sh`
    - Starts the local Kafka Connect stack using the cloud docker-compose file.
    - Options:
        - `--deploy_latest`: build and deploy the connector before starting.
    - After start, view logs:

```bash
docker compose -f src/integrationTest/docker/kafka-connect-cloud/docker-compose.yml logs -f
```

- `stop_local_kafka_connect.sh`
    - Stops and removes the compose stack started by the script above.

- `cleanup_connectors.sh`
    - Removes any connector definitions that was previously uploaded to this local Kafka Connect cluster
  
- `kafka/create_topic.sh`
    - Creates a topic in the broker container.
    - Run without arguments to see available flags and defaults. Example (with arguments):

```bash
./kafka/create_topic.sh --topic california-students
```

- `kafka/list_topics.sh`
    - Lists all topics in the broker. Example:

```bash
./kafka/list_topics.sh
```

- `kafka/generate_messages.sh`
    - Produces messages to a topic (CSV examples provided in `kafka/samples/`).
    - Run without arguments to see usage. Example (with arguments):

```bash
./kafka/generate_messages.sh --topic california-students --file ./kafka/samples/students_sample.csv
```

- `kafka/cleanup_all_topics.sh`
    - Deletes non-internal topics in the broker container.
    - By default, topics starting with `_` are excluded.
    - Kafka Connect internal Docker topics are always skipped and never deleted by this script:
        - Matches `docker-connect-(configs|offsets|status)` with optional suffix (e.g., `-cloud`).
    - Options (run with `--help` for details):
        - `--container NAME` (default: `kafka-cloud`)
        - `--include-internal` (still skips the Kafka Connect internal topics)
        - `--wait-seconds N`

### Notes
- Run any script without arguments to see available flags and defaults.
- CSV samples are in `kafka/samples/`.

