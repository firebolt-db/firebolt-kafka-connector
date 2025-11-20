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

## Docker Compose: Firebolt Core Image Override

The integration test Docker Compose stacks (Kafka Connect 3.9.1 and 4.0) accept an override for the Firebolt Core image via the `FIREBOLT_CORE_IMAGE` environment variable. If not provided, they default to `ghcr.io/firebolt-db/firebolt-core:preview-rc`.

Examples:

```bash
# Use default from GHCR
docker compose -f src/integrationTest/docker/kafka-connect-4.0/docker-compose.yml up -d

# Override with GHCR tag
FIREBOLT_CORE_IMAGE=ghcr.io/firebolt-db/firebolt-core:preview-rc \
docker compose -f src/integrationTest/docker/kafka-connect-4.0/docker-compose.yml up -d

# Override with AWS ECR image
FIREBOLT_CORE_IMAGE=123456789012.dkr.ecr.us-east-1.amazonaws.com/firebolt-core:preview-rc \
docker compose -f src/integrationTest/docker/kafka-connect-4.0/docker-compose.yml up -d
```

### Optional: Login to AWS ECR

If using an image hosted in AWS ECR, login first:

```bash
AWS_ACCOUNT_ID=123456789012 AWS_REGION=us-east-1 ./scripts/aws-ecr-login.sh
```

This script requires `aws` CLI v2 and `docker`. You can also pass `ECR_REGISTRY` explicitly if it differs from the default `<ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com`.

### CI: Passing the image via GitHub Actions

Set a repository variable `FIREBOLT_CORE_IMAGE` and pass it to compose. If using ECR, login first:

```yaml
jobs:
  it:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '11'
      # Configure AWS if using ECR
      - uses: aws-actions/configure-aws-credentials@v4
        if: ${{ startsWith(vars.FIREBOLT_CORE_IMAGE, 'aws_account_id') || contains(vars.FIREBOLT_CORE_IMAGE, '.dkr.ecr.') }}
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ vars.AWS_REGION }}
      - uses: aws-actions/amazon-ecr-login@v2
        if: ${{ contains(vars.FIREBOLT_CORE_IMAGE, '.dkr.ecr.') }}
      - name: Bring up stack
        env:
          FIREBOLT_CORE_IMAGE: ${{ vars.FIREBOLT_CORE_IMAGE }}
        run: |
          docker compose -f src/integrationTest/docker/kafka-connect-4.0/docker-compose.yml up -d
```
