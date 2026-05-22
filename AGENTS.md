# AGENTS.md

## Cursor Cloud specific instructions

This is a Java/Gradle project (Firebolt Kafka Sink Connector). The VM has Java 21 pre-installed which is compatible with the Java 11 source/target level.

### Build & Test Commands

| Action | Command |
|--------|---------|
| Build (no tests) | `./gradlew build -x test -x integrationTest --no-daemon` |
| Unit tests | `./gradlew test --no-daemon` |
| Code quality check | `./gradlew check --no-daemon` |
| Build uber JAR | `./gradlew jar --no-daemon` |
| Deploy to Kafka Connect | `./gradlew deployToKafkaConnect --no-daemon` |
| Build Confluent Hub archive | `./gradlew buildConfluentHubArchive --no-daemon` |
| Integration tests | See "Running Integration Tests" below |

### Running Integration Tests

Integration tests require Docker and the Kafka Connect Docker Compose stack running.

```bash
# 1. Start Docker daemon (if not running)
sudo dockerd &
sleep 5
sudo chmod 666 /var/run/docker.sock

# 2. Set environment variables
export FIREBOLT_CORE_IMAGE=ghcr.io/firebolt-db/firebolt-core:preview-rc
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export KAFKA_CONNECT_URL=http://localhost:8083
export KAFKA_CONNECT_VERSION=3.9.1

# 3. Build and deploy connector JAR
./gradlew deployToKafkaConnect --no-daemon

# 4. Set up firebolt-core data directory
mkdir -p src/integrationTest/docker/kafka-connect-3.9.1/firebolt-core
sudo chown 1111:1111 src/integrationTest/docker/kafka-connect-3.9.1/firebolt-core

# 5. Start Docker Compose stack
docker compose -f src/integrationTest/docker/kafka-connect-3.9.1/docker-compose.yml up -d

# 6. Wait for Kafka Connect to be ready
timeout 90 bash -c 'until curl -sf http://localhost:8083/connectors; do sleep 3; done'

# 7. Run integration tests
./gradlew integrationTest \
  -Djunit.jupiter.excludeTags=not_implemented,cloud \
  -Djunit.jupiter.includeTags=serialization \
  --tests '*.AllDataTypesSchemalessSerializerTest' \
  --no-daemon
```

To run ALL serialization integration tests, omit `--tests`:
```bash
./gradlew integrationTest -Djunit.jupiter.excludeTags=not_implemented,cloud -Djunit.jupiter.includeTags=serialization --no-daemon
```

### Key Notes

- **No dedicated linter** is configured (no checkstyle/spotbugs/pmd). The `./gradlew check` task runs compilation + unit tests + JaCoCo coverage.
- **Unit tests** run without Docker or external services.
- **Integration tests** require Docker with the Kafka Connect 3.9.1 (or 4.0) stack. The `FIREBOLT_CORE_IMAGE` is `ghcr.io/firebolt-db/firebolt-core:preview-rc` (publicly pullable).
- The Gradle wrapper (`./gradlew`) handles all builds — no Makefile or npm scripts.
- Use `--no-daemon` flag for one-off commands to avoid leftover Gradle daemons.
- The build uses `org.gradle.parallel=true` and caching by default (see `gradle.properties`).

### Docker in Cursor Cloud

Docker runs in nested mode (Docker-in-Docker inside Firecracker VM). Required configuration:
- Storage driver: `fuse-overlayfs` (configured in `/etc/docker/daemon.json`)
- iptables: must use `iptables-legacy` (not nftables)
- Socket permissions: after starting dockerd, run `sudo chmod 666 /var/run/docker.sock`

The update script handles Docker installation and configuration. Agents only need to start the daemon and services.
