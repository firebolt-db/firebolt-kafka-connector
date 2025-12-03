#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECT_URL="http://localhost:8083"
KAFKA_CONNECT_CONTAINER="kafka-connect-cloud"

usage() {
  cat <<EOF
Usage: $(basename "$0") [-u CONNECT_URL] [--container NAME]

Stops (pauses) and deletes ALL Kafka Connect connectors from the local docker container.

Options:
  -u, --url   Kafka Connect base URL (default: ${CONNECT_URL})
  --container Container name for docker exec fallback (default: ${KAFKA_CONNECT_CONTAINER})
  -h, --help  Show this help

Examples:
  $(basename "$0")
  $(basename "$0") -u http://localhost:8083
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -u|--url)
      CONNECT_URL="${2:-}"
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

if ! docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONNECT_CONTAINER}"; then
  echo "Kafka Connect container '${KAFKA_CONNECT_CONTAINER}' not running. Start it with demo/start_local_kafka_connect.sh." >&2
  exit 1
fi

echo "Fetching connectors from ${CONNECT_URL}/connectors ..."
CONNECTORS_JSON="$(curl -sS "${CONNECT_URL}/connectors" || echo "")"

# If host call failed or returned non-array, try inside the container as a fallback
if [[ -z "${CONNECTORS_JSON}" || "${CONNECTORS_JSON}" == "{}" ]]; then
  echo "Host query returned empty/invalid response. Trying inside container '${KAFKA_CONNECT_CONTAINER}' ..."
  CONNECTORS_JSON="$(docker exec -i "${KAFKA_CONNECT_CONTAINER}" curl -sS http://localhost:8083/connectors || echo "")"
fi

CONNECTOR_NAMES="$(
  printf '%s' "${CONNECTORS_JSON}" | python3 -c 'import sys,json
try:
    data=json.load(sys.stdin)
except Exception:
    data=[]
if isinstance(data,list):
    for name in data:
        if isinstance(name,str):
            print(name)'
)"

if [[ -z "${CONNECTOR_NAMES}" ]]; then
  echo "No connectors found."
  # Print raw response to aid debugging
  if [[ -n "${CONNECTORS_JSON}" ]]; then
    echo "Raw response:"
    echo "${CONNECTORS_JSON}"
  fi
  exit 0
fi

echo "Found connectors:"
echo "${CONNECTOR_NAMES}"

while IFS= read -r NAME; do
  [[ -z "${NAME}" ]] && continue
  echo "Pausing connector '${NAME}' ..."
  curl -sS -X PUT "${CONNECT_URL}/connectors/${NAME}/pause" -o /dev/null || true

  echo "Deleting connector '${NAME}' ..."
  HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" -X DELETE "${CONNECT_URL}/connectors/${NAME}" || true)
  if [[ "${HTTP_CODE}" == "200" || "${HTTP_CODE}" == "204" ]]; then
    echo "Deleted '${NAME}'."
  else
    echo "Failed to delete '${NAME}' (HTTP ${HTTP_CODE})." >&2
  fi
done <<< "${CONNECTOR_NAMES}"

echo "Cleanup complete."


