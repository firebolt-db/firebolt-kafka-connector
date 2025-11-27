#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--deploy_latest]

Starts the Kafka Connect cloud stack using the integration docker-compose file.

Options:
  --deploy_latest  If set, runs Gradle task 'deployToKafkaConnect' before starting (default: false)
  -h, --help     Show this help

Examples:
  $(basename "$0")                  # start cloud stack
  $(basename "$0") --deploy_latest  # deploy latest jar then start cloud stack

Tip: Build and deploy the connector jar first:
  ./gradlew deployToKafkaConnect
EOF
}

DEPLOY_LATEST="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy_latest)
      DEPLOY_LATEST="true"
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

COMPOSE_FILE="${PROJECT_ROOT}/src/integrationTest/docker/kafka-connect-cloud/docker-compose.yml"

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "docker-compose file not found: ${COMPOSE_FILE}" >&2
  exit 1
fi

if [[ "${DEPLOY_LATEST}" == "true" ]]; then
  echo "Building and deploying connector jar to Kafka Connect plugin directories..."
  if [[ -x "${PROJECT_ROOT}/gradlew" ]]; then
    ( cd "${PROJECT_ROOT}" && ./gradlew deployToKafkaConnect )
  else
    ( cd "${PROJECT_ROOT}" && gradle deployToKafkaConnect )
  fi
else
  echo "Skipping deployToKafkaConnect (use --deploy_latest to enable)."
fi

echo "Starting Kafka Connect cloud stack..."
docker compose -f "${COMPOSE_FILE}" up -d --remove-orphans
echo "Started. View logs with:"
echo "  docker compose -f \"${COMPOSE_FILE}\" logs -f"




