# AGENTS.md

## Cursor Cloud specific instructions

### Running Integration Tests

Integration tests need Docker and the Kafka Connect Docker Compose stack. The update script installs Docker; agents must start the daemon and bring up services:

```bash
sudo dockerd &
sleep 5
sudo chmod 666 /var/run/docker.sock

export FIREBOLT_CORE_IMAGE=ghcr.io/firebolt-db/firebolt-core:preview-rc
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export KAFKA_CONNECT_URL=http://localhost:8083
export KAFKA_CONNECT_VERSION=3.9.1

./gradlew deployToKafkaConnect --no-daemon

mkdir -p src/integrationTest/docker/kafka-connect-3.9.1/firebolt-core
sudo chown 1111:1111 src/integrationTest/docker/kafka-connect-3.9.1/firebolt-core

docker compose -f src/integrationTest/docker/kafka-connect-3.9.1/docker-compose.yml up -d
timeout 90 bash -c 'until curl -sf http://localhost:8083/connectors; do sleep 3; done'

./gradlew integrationTest -Djunit.jupiter.excludeTags=not_implemented,cloud --no-daemon
```

### Non-obvious gotchas

- `FIREBOLT_CORE_IMAGE` is `ghcr.io/firebolt-db/firebolt-core:preview-rc` (publicly pullable). This matches the GitHub Actions `vars.FIREBOLT_CORE_IMAGE` repo variable.
- The firebolt-core container runs as UID 1111 — the mounted data directory must be `chown 1111:1111`.
- Docker runs nested (DinD in Firecracker). Requires `fuse-overlayfs` storage driver and `iptables-legacy`. The update script configures this; if dockerd fails to start, check `/etc/docker/daemon.json` and iptables alternatives.
- No dedicated linter exists — `./gradlew check` is compilation + tests + JaCoCo.
