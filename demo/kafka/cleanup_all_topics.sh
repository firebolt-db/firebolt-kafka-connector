#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_CONTAINER="kafka-cloud"
BOOTSTRAP="kafka:29092"
INCLUDE_INTERNAL="false"
WAIT_SECONDS=30

usage() {
  cat <<EOF
Usage: $(basename "$0") [--container NAME] [--include-internal] [--wait-seconds N]

Deletes all Kafka topics from the broker in the cloud demo container.
By default, internal topics (names starting with '_') are NOT deleted.
Additionally, Kafka Connect internal Docker topics matching
  docker-connect-(configs|offsets|status)(-<suffix>)?
are NEVER deleted by this script (e.g., 'docker-connect-configs', 'docker-connect-configs-cloud').

Options:
  --container NAME     Kafka broker container name (default: ${KAFKA_CONTAINER})
  --include-internal   Also delete internal topics (names starting with '_')
  --wait-seconds N     Seconds to wait for async deletions (default: ${WAIT_SECONDS})
  -h, --help           Show this help

Examples:
  $(basename "$0")
  $(basename "$0") --include-internal
  $(basename "$0") --container kafka-cloud
  $(basename "$0") --wait-seconds 60
  $(basename "$0") --include-internal --container kafka-cloud --wait-seconds 60
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container)
      KAFKA_CONTAINER="${2:-}"
      shift 2
      ;;
    --include-internal)
      INCLUDE_INTERNAL="true"
      shift 1
      ;;
    --wait-seconds)
      WAIT_SECONDS="${2:-30}"
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

if ! docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONTAINER}"; then
  echo "Kafka container '${KAFKA_CONTAINER}' not running. Start the environment first with demo/start_local_kafka_connect.sh." >&2
  exit 1
fi

echo "Listing topics from '${KAFKA_CONTAINER}' ..."
# Build list robustly for macOS bash 3.x (no mapfile): strip CRs, drop blanks, optionally filter internal
if [[ "${INCLUDE_INTERNAL}" == "true" ]]; then
  TOPICS="$(docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list" | tr -d '\r' | awk 'NF' | grep -vE '^docker-connect-(configs|offsets|status)(-.+)?$')"
else
  TOPICS="$(docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list" | tr -d '\r' | awk 'NF' | grep -vE '^_' | grep -vE '^docker-connect-(configs|offsets|status)(-.+)?$')"
fi

if [[ -z "${TOPICS}" ]]; then
  echo "No topics to delete."
  exit 0
fi

echo "Topics to delete:"
printf '%s\n' "${TOPICS}"

echo "Note: Kafka topic deletions are asynchronous. If Kafka Connect is running, internal topics (docker-connect-*) may be recreated automatically."
echo "If you want a clean slate, consider stopping the environment first: bash demo/stop_local_kafka_connect.sh"

printf '%s\n' "${TOPICS}" | while IFS= read -r TOPIC; do
  [[ -z "${TOPIC}" ]] && continue
  echo "Deleting topic '${TOPIC}' ..."
  docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --delete --topic '${TOPIC}'" || true

  # Wait for deletion to complete (poll up to WAIT_SECONDS)
  end=$((SECONDS + WAIT_SECONDS))
  while (( SECONDS < end )); do
    present="$(docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list | grep -x '${TOPIC}' || true")"
    if [[ -z "${present}" ]]; then
      echo "Confirmed deletion of '${TOPIC}'."
      break
    fi
    sleep 1
  done
  # Final check
  present="$(docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list | grep -x '${TOPIC}' || true")"
  if [[ -n "${present}" ]]; then
    echo "Topic '${TOPIC}' still present after waiting ${WAIT_SECONDS}s (it may be being recreated or deletion pending)."
  fi
done

echo "Deletion requests submitted. Current topics:"
docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list" || true

echo "Done."


