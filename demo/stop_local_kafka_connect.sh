#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<EOF
Usage: $(basename "$0")

Stops the Kafka Connect cloud stack started from integration docker-compose files.

Options:
  -h, --help     Show this help

Examples:
  $(basename "$0")            # stop cloud stack
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

COMPOSE_FILE="${PROJECT_ROOT}/src/integrationTest/docker/kafka-connect-cloud/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "docker-compose file not found: ${COMPOSE_FILE}" >&2
  exit 1
fi

echo "Stopping Kafka Connect cloud stack..."
docker compose -f "${COMPOSE_FILE}" down
echo "Stopped."




