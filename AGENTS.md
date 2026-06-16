# AGENTS.md

## Cursor Cloud specific instructions

### Running Integration Tests

Integration tests need Docker and the Kafka Connect Docker Compose stack. The update script installs Docker; agents must start the daemon and bring up services:

```bash
sudo dockerd &
sleep 5
sudo chmod 666 /var/run/docker.sock

# Optional: the compose file pins a sensible default, so this export is only needed to override it.
export FIREBOLT_ENGINE_IMAGE=ghcr.io/firebolt-db/engine:release-5.0.1-0.20260615205019.4f5c69172cd3
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export KAFKA_CONNECT_URL=http://localhost:8083
export KAFKA_CONNECT_VERSION=3.9.1

./gradlew deployToKafkaConnect --no-daemon

mkdir -p src/integrationTest/docker/kafka-connect-3.9.1/firebolt-core
sudo chown 3473:3473 src/integrationTest/docker/kafka-connect-3.9.1/firebolt-core

docker compose -f src/integrationTest/docker/kafka-connect-3.9.1/docker-compose.yml up -d
timeout 90 bash -c 'until curl -sf http://localhost:8083/connectors; do sleep 3; done'

./gradlew integrationTest -Djunit.jupiter.excludeTags=not_implemented,cloud --no-daemon
```

### Non-obvious gotchas

- The engine image is `ghcr.io/firebolt-db/engine` (the OSS artefact from `firebolt-kubernetes-operator`, replacing Firebolt Core; publicly pullable). The docker-compose files pin a default tag; override it with the `FIREBOLT_ENGINE_IMAGE` env var locally or the `vars.FIREBOLT_ENGINE_IMAGE` GitHub Actions repo variable.
- The engine container runs as the `firebolt` user (UID:GID 3473) and stores its data under `/firebolt-core/volume` — the mounted host directory must be `chown 3473:3473`.
- Docker runs nested (DinD in Firecracker). Requires `fuse-overlayfs` storage driver and `iptables-legacy`. The update script configures this; if dockerd fails to start, check `/etc/docker/daemon.json` and iptables alternatives.
- No dedicated linter exists — `./gradlew check` is compilation + tests + JaCoCo.
- PRs are squash-merged to `main`. Conventional commits preferred (`feat:`, `fix:`, `chore:`, `docs:`, `test:`).
- Cloud integration tests (tagged `cloud`) need additional secrets: `firebolt.clientId`, `firebolt.clientSecret`, `firebolt.jdbc.url`, plus Confluent Cloud vars (`confluent.environment.id`, `confluent.cluster.id`, `confluent.firebolt.connector.plugin.id`). These are excluded by default with `-Djunit.jupiter.excludeTags=not_implemented,cloud`.
