# Firebolt Kafka Connector

A Java project using Gradle build system with the main package `com.firebolt`.

## Project Structure

```
firebolt-kafka-connector/
├── build.gradle                   # Gradle build configuration
├── settings.gradle                # Gradle settings
├── gradlew                        # Gradle wrapper script (Unix)
├── gradlew.bat                    # Gradle wrapper script (Windows)
├── gradle/wrapper/                # Gradle wrapper files
├── src/
│   ├── main/
│   │   ├── java/com/firebolt/     # Main Java source code
│   │   └── resources/             # Main resources (logback.xml, etc.)
│   └── test/
│       ├── java/com/firebolt/     # Test Java source code
│       └── resources/             # Test resources
├── logs/                          # Application log files
└── .gitignore                     # Git ignore patterns
```

## Building and Running

### Prerequisites
- Java 11 or higher

### Build the project
```bash
./gradlew build
```

### Run tests
```bash
./gradlew test
```

### Run the application
```bash
./gradlew run
```

### Clean build artifacts
```bash
./gradlew clean
```

## Dependencies

- **Kafka Clients**: Apache Kafka client library
- **SLF4J**: Simple Logging Facade for Java
- **Logback**: Logging framework
- **JUnit 5**: Testing framework
- **Mockito**: Mocking framework for tests

## Development

The main application entry point is `com.firebolt.Main.java`. 

Configuration is managed through `src/main/resources/logback.xml` for logging settings.

Tests are located in `src/test/java/com/firebolt/` and follow JUnit 5 conventions.
