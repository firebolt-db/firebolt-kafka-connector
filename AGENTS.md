# AGENTS.md

## Cursor Cloud specific instructions

This is a Java/Gradle project (Firebolt Kafka Sink Connector). The VM has Java 21 pre-installed which is compatible with the Java 11 source/target level.

### Build & Test Commands

| Action | Command |
|--------|---------|
| Build (no tests) | `./gradlew build -x test -x integrationTest` |
| Unit tests | `./gradlew test` |
| Code quality check | `./gradlew check` |
| Build uber JAR | `./gradlew jar` |
| Build Confluent Hub archive | `./gradlew buildConfluentHubArchive` |
| Integration tests (requires Docker) | `./gradlew integrationTest -Djunit.jupiter.excludeTags=not_implemented,cloud` |

### Key Notes

- **No dedicated linter** is configured (no checkstyle/spotbugs/pmd). The `./gradlew check` task runs compilation + unit tests + JaCoCo coverage.
- **Unit tests** (1365 tests) run without Docker or external services.
- **Integration tests** require Docker and use TestContainers. They also need the `FIREBOLT_CORE_IMAGE` env var pointing to a private Docker image.
- The Gradle wrapper (`./gradlew`) handles all builds — no Makefile or npm scripts.
- Use `--no-daemon` flag for one-off commands to avoid leftover Gradle daemons, or omit it if running multiple builds in sequence (daemon speeds up subsequent builds).
- The build uses `org.gradle.parallel=true` and caching by default (see `gradle.properties`).
