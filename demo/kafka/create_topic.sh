#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONNECT_CONTAINER="kafka-cloud"
BOOTSTRAP="kafka:29092"
PARTITIONS=1
REPLICATION=1
RETENTION_MS=86400000

usage() {
  cat <<EOF
Usage: $(basename "$0") -t TOPIC [--container NAME]

Creates a Kafka topic inside the demo cloud environment.
Defaults: partitions=${PARTITIONS}, replication-factor=${REPLICATION}, retention.ms=${RETENTION_MS} (1 day)

Required:
  -t, --topic       Topic name to create

Optional:
  --container       Kafka container name (default: ${KAFKA_CONNECT_CONTAINER})
  -h, --help        Show this help

Examples:
  $(basename "$0") -t demo-topic
  $(basename "$0") -t demo-topic --container kafka-cloud
EOF
}

TOPIC=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--topic)
      TOPIC="${2:-}"
      shift 2
      ;;
    --container)
      KAFKA_CONNECT_CONTAINER="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${TOPIC}" ]]; then
  echo "Error: --topic is required." >&2
  usage
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONNECT_CONTAINER}"; then
  echo "Kafka container '${KAFKA_CONNECT_CONTAINER}' not running. Start the environment first with demo/start_local_kafka_connect.sh." >&2
  exit 1
fi

echo "Creating topic '${TOPIC}' (partitions=${PARTITIONS}, rf=${REPLICATION}, retention.ms=${RETENTION_MS}) ..."
docker exec -i "${KAFKA_CONNECT_CONTAINER}" bash -lc "\
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
               --create \
               --topic '${TOPIC}' \
               --partitions ${PARTITIONS} \
               --replication-factor ${REPLICATION} \
               --config retention.ms=${RETENTION_MS} \
  " || true

echo "Describing topic '${TOPIC}' ..."
docker exec -i "${KAFKA_CONNECT_CONTAINER}" bash -lc "\
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
               --describe \
               --topic '${TOPIC}' \
  " || true

echo "Done."


