# Obvious repo guidance

<!-- obvious-install: skill=autobuild-setup, skill-version=1.0.1, template-version=1 -->

This repo uses `.obvious/` for reviewed Autobuild guidance.

Before editing, suggest the smallest relevant set of `.obvious` files for the task. Match candidate files by reading their frontmatter.

## Codebase Map

See `.obvious/codebase-map.md`.

## Repo Guidance for Autobuild

<!-- synthesized from: README.md, QUICKSTART.md, build.gradle, gradle.properties, .github/workflows/ -->

**Stack:** Java 11+ (built with Java 21 in sandbox), Gradle 8.x, Kafka Connect API 3.x/4.x.

**Build commands:**
- `./gradlew build` — compile + unit tests + jar (skips integration tests)
- `./gradlew test` — unit tests only (1386 tests)
- `./gradlew build -x integrationTest` — build without integration tests
- `./gradlew buildConfluentHubArchive` — build Confluent Hub ZIP at `build/confluent/`
- `./gradlew deployToKafkaConnect` — build jar + deploy to local Docker plugin dirs

**Test commands:**
- Unit tests: `./gradlew test` (no external deps required)
- Integration tests: `./gradlew integrationTest` (requires Docker, Kafka, Firebolt credentials)
- Integration tests use TestContainers (Docker) for Kafka; require real Firebolt credentials for cloud tests

**Key env vars for integration tests:**
- `firebolt.clientId` — Firebolt service account client ID (secret)
- `firebolt.clientSecret` — Firebolt service account client secret (secret)
- `firebolt.jdbc.url` — JDBC URL e.g. `jdbc:firebolt:mydb?engine=myengine&account=myaccount`
- Confluent Cloud vars: `confluent.environment.id`, `confluent.cluster.id`, `confluent.firebolt.connector.plugin.id`

**Connector class:** `com.firebolt.kafka.connect.FireboltSinkConnector`

**Ingestion modes:** `sql` (default, JDBC INSERT) or `binary` (Parquet upload)

**CI:** GitHub Actions — build.yml (unit tests + build), integration test workflows require secrets. Unit tests run on every push; integration tests are separately triggered.

**Release:** GitHub Releases with uber-jar, Confluent Hub archive ZIP, and sources jar. Squash-merge PRs to main.

**No local dev server** — this is a library/plugin, not a running service. Local validation is done via `./gradlew build` + unit tests. Full integration testing requires Docker + Firebolt account.

**Commit style:** Conventional commits preferred (feat:, fix:, chore:, docs:, test:).

## Sandbox Snapshot

- **Snapshot ID:** `qln0ulivc01k7y6w751w:default` (template — existing snapshot updated)
- **Captured:** `2026-05-21T19:00:00Z`
- **Dev stack healthy:** yes — 1386 unit tests pass, build succeeds
- **Java version:** OpenJDK 21.0.11 (installed during setup; repo requires Java 11+)

## Bibliography

Scanned 2026-05-21. Key product nodes registered:

| Node | Type | Description |
|---|---|---|
| Firebolt Kafka Sink Connector | integration | Top-level connector product (v0.4.4) |
| FireboltSinkConnector | system | Main connector class / entry point |
| SQL Ingestion Mode | feature | Default JDBC INSERT ingestion path |
| Binary / Parquet Ingestion Mode | feature | Parquet-based bulk upload ingestion path |

## Security Scan

> **Note:** security_scan_not_triggered — repository `firebolt-db/firebolt-kafka-connector` not yet connected in Obvious workspace (Settings → Repositories required). Trigger manually using the `trigger_security_onboarding` tool with commit SHA `68e0b56641acb32498c5ed046bd88e9fb925fec2` after connecting.

## Runbooks

Populated by autobuild-runbooks skill when requested. See `.obvious/runbooks/` after that skill runs.
