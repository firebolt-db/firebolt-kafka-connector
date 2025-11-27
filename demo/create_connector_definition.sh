#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEF_FILE=""
CONNECT_URL="http://localhost:8083"
KAFKA_CONNECT_CONTAINER="kafka-connect-cloud"

usage() {
  cat <<EOF
Usage: $(basename "$0") -f DEFINITION_JSON [-u CONNECT_URL]

Creates (or updates if already exists) a Kafka Connect connector using the provided definition JSON.
The JSON should contain the full payload expected by POST /connectors, e.g.:
  { "name": "my-connector", "config": { ... } }

By default it will POST to ${CONNECT_URL}/connectors. If the connector already exists (409),
it will PUT the "config" to /connectors/{name}/config to update it.

Required:
  -f, --file       Path to connector definition JSON

Optional:
  -u, --url        Kafka Connect base URL (default: ${CONNECT_URL})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)
      DEF_FILE="${2:-}"
      shift 2
      ;;
    -u|--url)
      CONNECT_URL="${2:-}"
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

if [[ -z "${DEF_FILE}" ]]; then
  echo "Error: --file is required." >&2
  usage
  exit 1
fi

if [[ ! -f "${DEF_FILE}" ]]; then
  echo "Error: definition file not found: ${DEF_FILE}" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONNECT_CONTAINER}"; then
  echo "Kafka Connect container '${KAFKA_CONNECT_CONTAINER}' not running. Start it with demo/start.sh." >&2
  exit 1
fi

echo "Creating connector via ${CONNECT_URL}/connectors ..."
TMP_RESP="$(mktemp)"
HTTP_CODE=$(curl -sS -o "${TMP_RESP}" -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  --data-binary @"${DEF_FILE}" \
  "${CONNECT_URL}/connectors" || true)

if [[ "${HTTP_CODE}" == "201" || "${HTTP_CODE}" == "200" ]]; then
  echo "Connector created successfully."
  rm -f "${TMP_RESP}"
  exit 0
fi

if [[ "${HTTP_CODE}" == "409" ]]; then
  echo "Connector already exists. Attempting to update config ..."
  CONNECTOR_NAME="$(python3 - <<PY
import json,sys
with open("${DEF_FILE}", 'r', encoding='utf-8') as f:
    data=json.load(f)
print(data.get('name',''))
PY
)"
  if [[ -z "${CONNECTOR_NAME}" ]]; then
    echo "Failed to determine connector name from ${DEF_FILE} to perform update." >&2
    echo "Response body:" >&2
    cat "${TMP_RESP}" >&2
    rm -f "${TMP_RESP}"
    exit 1
  fi
  TMP_CFG="$(mktemp)"
  python3 - <<PY > "${TMP_CFG}"
import json,sys
with open("${DEF_FILE}", 'r', encoding='utf-8') as f:
    data=json.load(f)
cfg=data.get('config',{})
print(json.dumps(cfg))
PY
  HTTP_CODE_PUT=$(curl -sS -o "${TMP_RESP}" -w "%{http_code}" -X PUT \
    -H "Content-Type: application/json" \
    --data-binary @"${TMP_CFG}" \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" || true)
  rm -f "${TMP_CFG}"
  if [[ "${HTTP_CODE_PUT}" == "200" ]]; then
    echo "Connector '${CONNECTOR_NAME}' updated successfully."
    rm -f "${TMP_RESP}"
    exit 0
  else
    echo "Failed to update connector '${CONNECTOR_NAME}'. HTTP ${HTTP_CODE_PUT}" >&2
    echo "Response body:" >&2
    cat "${TMP_RESP}" >&2
    rm -f "${TMP_RESP}"
    exit 1
  fi
fi

echo "Failed to create connector. HTTP ${HTTP_CODE}" >&2
echo "Response body:" >&2
cat "${TMP_RESP}" >&2
rm -f "${TMP_RESP}"
exit 1


