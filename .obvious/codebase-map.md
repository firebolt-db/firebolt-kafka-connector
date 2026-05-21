# Codebase Map

| Directory | Purpose |
|---|---|
| `src/main/java` | Connector source — sink connector, task, ingestion, config, converters, data types |
| `src/main/resources` | META-INF connector registration, version.properties template |
| `src/test/java` | Unit tests (1386 tests, JUnit 5 + Mockito, no external deps) |
| `src/integrationTest/java` | Integration tests — TestContainers Kafka, Firebolt cloud tests, load tests |
| `src/integrationTest/docker` | Docker Compose stacks for Kafka Connect 3.9.1, 4.0, and Confluent Cloud setups |
| `src/integrationTest/resources` | Integration test configs, schemas, SQL scripts |
| `config/archive` | Confluent Hub manifest and archive assets for publishing |
| `demo` | Local Kafka Connect demo scripts — start/stop stack, create topics, sample connector JSON |
| `gradle` | Gradle wrapper JARs and properties |
| `.github/workflows` | CI — build, unit tests, integration tests, release, load tests, Confluent Hub publish |
