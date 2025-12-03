#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONTAINER="kafka-cloud"
BOOTSTRAP="kafka:29092"
INCLUDE_INTERNAL="true"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--container NAME] [--exclude-internal]

Lists Kafka topics from the broker running in the demo docker container.

Options:
  --container NAME      Kafka broker container name (default: ${KAFKA_CONTAINER})
  --exclude-internal    Hide internal topics (names starting with '_')
  -h, --help            Show this help

Examples:
  $(basename "$0")
  $(basename "$0") --exclude-internal
  $(basename "$0") --container kafka-cloud
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container)
      KAFKA_CONTAINER="${2:-}"
      shift 2
      ;;
    --exclude-internal)
      INCLUDE_INTERNAL="false"
      shift 1
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

if ! docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONTAINER}"; then
  echo "Kafka container '${KAFKA_CONTAINER}' not running. Start the environment first with demo/start_local_kafka_connect.sh." >&2
  exit 1
fi

ALL_TOPICS="$(docker exec -i "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list" || true)"

if [[ -z "${ALL_TOPICS}" ]]; then
  echo "No topics found."
  exit 0
fi

if [[ "${INCLUDE_INTERNAL}" == "true" ]]; then
  printf '%s\n' "${ALL_TOPICS}"
else
  printf '%s\n' "${ALL_TOPICS}" | grep -vE '^_'
fi


