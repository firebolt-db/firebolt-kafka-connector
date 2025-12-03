#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat <<EOF
Usage: $(basename "$0") -f CSV_FILE -t TOPIC [--batch-size N]

Publishes JSON messages to a Kafka topic from a CSV file.
Each CSV row becomes a JSON object using the header row as attribute names.
Targets a running demo docker environment (start with demo/start_local_kafka_connect.sh).

Required:
  -f, --file     Path to CSV file
  -t, --topic    Kafka topic name

Optional:
  -b, --batch-size N  Send messages in batches of N (default: 100). Each batch is sent once per second.
  -h, --help     Show this help

Examples:
  $(basename "$0") -f ./data/events.csv -t demo-topic
  $(basename "$0") -f ./data/events.csv -t demo-topic --batch-size 250
EOF
}

CSV_FILE=""
TOPIC=""
BATCH_SIZE=100

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)
      CSV_FILE="${2:-}"
      shift 2
      ;;
    -t|--topic)
      TOPIC="${2:-}"
      shift 2
      ;;
    -b|--batch-size)
      BATCH_SIZE="${2:-}"
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

if [[ -z "${CSV_FILE}" || -z "${TOPIC}" ]]; then
  echo "Both --file and --topic are required." >&2
  usage
  exit 1
fi

if [[ ! -f "${CSV_FILE}" ]]; then
  echo "CSV file not found: ${CSV_FILE}" >&2
  exit 1
fi

KAFKA_CONTAINER="kafka-cloud"

if ! docker ps --format '{{.Names}}' | grep -qx "${KAFKA_CONTAINER}"; then
  echo "Kafka container '${KAFKA_CONTAINER}' not running. Start the environment first with demo/start_local_kafka_connect.sh." >&2
  exit 1
fi

echo "Publishing messages from '${CSV_FILE}' to topic '${TOPIC}' on cloud target..."
echo "Batch size: ${BATCH_SIZE} messages; sending one batch per second."

# Convert CSV to JSON lines and write to a temp file
TMP_DIR="$(mktemp -d)"
JSON_FILE="${TMP_DIR}/messages.jsonl"

python3 - "$CSV_FILE" > "${JSON_FILE}" <<'PYCODE'
import csv, json, sys
csv_path = sys.argv[1]
with open(csv_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        normalized = {k: (v if v != '' else None) for k, v in row.items()}
        sys.stdout.write(json.dumps(normalized, ensure_ascii=False) + "\n")
PYCODE

# Split into batches of BATCH_SIZE
split -l "${BATCH_SIZE}" -a 5 -d "${JSON_FILE}" "${TMP_DIR}/batch_"

# Produce each batch once per second and print batch number after each
BATCH_NUM=1
for BATCH_FILE in $(ls "${TMP_DIR}"/batch_* 2>/dev/null | sort); do
  COUNT="$(wc -l < "${BATCH_FILE}" | tr -d '[:space:]')"
  if [[ "${COUNT}" -eq 0 ]]; then
    continue
  fi
  docker exec -i "${KAFKA_CONTAINER}" bash -lc "kafka-console-producer --bootstrap-server kafka:29092 --topic '${TOPIC}' >/dev/null" < "${BATCH_FILE}"
  echo "Processed batch ${BATCH_NUM} (${COUNT} messages)."
  BATCH_NUM=$((BATCH_NUM + 1))
  # Wait 1 second before sending the next batch (skip sleep after the last batch)
  :
  # shellcheck disable=SC2046
  if [[ "${BATCH_FILE}" != $(ls "${TMP_DIR}"/batch_* 2>/dev/null | sort | tail -n 1) ]]; then
    sleep 1
  fi
done

# Cleanup
rm -rf "${TMP_DIR}"

echo "Done."


