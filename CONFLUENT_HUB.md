# Confluent Hub Archive for Firebolt Kafka Connector

This document explains how to build and publish the Firebolt Kafka Connector to Confluent Hub.

## Overview

The Firebolt Kafka Connector is packaged as a Confluent Hub archive that can be uploaded to [Confluent Hub](https://docs.confluent.io/platform/current/connect/confluent-hub/component-archive.html#) for distribution.

## Archive Structure

The Confluent Hub archive follows the standard structure:

```
firebolt-kafka-connect-{version}.zip
├── manifest.json          # Component metadata and configuration
├── lib/                   # JAR files containing the connector
│   └── firebolt-kafka-connector-{version}-confluent.jar
├── doc/                   # Documentation files
│   ├── README.md
│   └── LICENSE
└── assets/                # Images and logos
    ├── firebolt_logo.png
    └── apache_logo.gif
```

## Building the Archive

### Prerequisites

1. Ensure you have the required logo files in `config/archive/assets/`:
   - `firebolt_logo.png` (at least 400x200 pixels)
   - `apache_logo.png` (Apache License logo)

2. Update the manifest.json file in `config/archive/` if needed

### Build Commands

```bash
# Build the complete Confluent Hub archive
./gradlew buildConfluentHubArchive

# Or build individual components
./gradlew confluentJar                    # Build the fat JAR
./gradlew prepareConfluentArchive         # Prepare archive structure
./gradlew createConfluentArchive          # Create the ZIP file
```

### Output

The archive will be created at:
```
build/confluent/firebolt-kafka-connect-{version}.zip
```

## Manifest Configuration

The `config/archive/manifest.json` file contains all the metadata for Confluent Hub:

### Key Fields

- **component_types**: `["sink"]` - Identifies this as a sink connector
- **name**: `"firebolt-kafka-connect"` - Internal name for the component
- **title**: `"Firebolt Kafka Connect Sink Connector"` - Display name
- **version**: `${version}` - Automatically expanded from gradle.properties
- **release_date**: `${releaseDate}` - Automatically set to current date (ISO format)
- **description**: Detailed description of features and capabilities
- **owner**: Firebolt organization information
- **features**: Technical capabilities and guarantees
- **requirements**: System and software requirements
- **support**: Support information and contact details
- **tags**: Searchable tags for discovery

### Customization

You can customize the manifest by editing `config/archive/manifest.json`:

1. **Update URLs**: Change documentation and support URLs
2. **Modify description**: Update features and capabilities
3. **Add requirements**: List additional system requirements
4. **Update tags**: Add relevant search tags

## Publishing to Confluent Hub

### Prerequisites

1. Create a Confluent Hub account at [hub.confluent.io](https://hub.confluent.io)
2. Register as a component owner
3. Ensure you have the necessary permissions

### Upload Process

1. **Build the archive**:
   ```bash
   ./gradlew buildConfluentHubArchive
   ```

2. **Upload to Confluent Hub**:
   - Go to [hub.confluent.io](https://hub.confluent.io)
   - Navigate to "Upload Component"
   - Upload the ZIP file from `build/confluent/`

3. **Review and publish**:
   - Confluent will review the component
   - Once approved, it will be available on Confluent Hub

## Version Management

### Updating Versions

1. **Update version in gradle.properties**:
   ```properties
   version=0.2
   ```

2. **Update manifest.json** if needed:
   - Update description for new features
   - Add new requirements
   - Update documentation URLs

3. **Build new archive**:
   ```bash
   ./gradlew buildConfluentHubArchive
   ```

### Version Naming Convention

- Use semantic versioning (e.g., 0.1.0, 0.2.0, 1.0.0)
- Archive filename: `firebolt-kafka-connect-{version}.zip`
- JAR filename: `firebolt-kafka-connector-{version}-confluent.jar`

## Troubleshooting

### Common Issues

1. **Missing logo files**:
   ```
   Error: Logo files not found in assets/
   ```
   Solution: Ensure `firebolt_logo.png` and `apache_logo.gif` exist in `config/archive/assets/`

2. **Manifest validation errors**:
   ```
   Error: Invalid manifest.json
   ```
   Solution: Validate the JSON syntax and required fields

3. **Archive too large**:
   ```
   Error: Archive exceeds size limit
   ```
   Solution: Review dependencies and exclude unnecessary files

### Validation

Before uploading, validate your archive:

```bash
# Check archive structure
unzip -l build/confluent/firebolt-kafka-connect-{version}.zip

# Validate manifest.json
unzip -p build/confluent/firebolt-kafka-connect-{version}.zip firebolt-kafka-connect-{version}/manifest.json | jq .

# Check JAR contents
jar -tf build/libs/firebolt-kafka-connector-{version}-confluent.jar | head -20
```

## References

- [Confluent Hub Component Archive Documentation](https://docs.confluent.io/platform/current/connect/confluent-hub/component-archive.html#)
- [Confluent Hub Upload Guide](https://docs.confluent.io/platform/current/connect/confluent-hub/upload.html)
- [Kafka Connect Plugin Development](https://kafka.apache.org/documentation/#connect_development) 